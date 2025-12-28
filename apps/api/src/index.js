import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Queue } from "bullmq";
import { desc, sql } from "drizzle-orm";
import cors from "@fastify/cors";
import Fastify from "fastify";
import OpenAI from "openai";
import YAML from "yaml";
import {
  JOB_TYPES,
  QUEUE_NAMES,
  chunks,
  getRedisConnectionOptions,
  ingestJobs,
  sources
} from "@app/shared";
import { db } from "./db/index.js";

const app = Fastify({ logger: true });

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..", "..");
const projectsFile = (() => {
  const configured = process.env.PROJECTS_FILE;
  if (!configured) {
    return path.join(repoRoot, "projects.yaml");
  }

  return path.isAbsolute(configured)
    ? configured
    : path.join(repoRoot, configured);
})();

const corsOrigin = process.env.CORS_ORIGIN
  ? process.env.CORS_ORIGIN.split(",").map((origin) => origin.trim())
  : true;

app.register(cors, { origin: corsOrigin });

const resolveCorsOrigin = (request) => {
  const originHeader = request.headers.origin;
  if (!originHeader) {
    return null;
  }
  if (corsOrigin === true) {
    return originHeader;
  }
  if (Array.isArray(corsOrigin)) {
    return corsOrigin.includes(originHeader) ? originHeader : null;
  }
  if (typeof corsOrigin === "string") {
    return corsOrigin === originHeader ? originHeader : null;
  }
  return null;
};

const adminApiKey = process.env.ADMIN_API_KEY;
const ingestQueue = new Queue(QUEUE_NAMES.ingest, {
  connection: getRedisConnectionOptions()
});

const openaiApiKey = process.env.OPENAI_API_KEY;
const embeddingModel =
  process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";
const chatModel = process.env.OPENAI_CHAT_MODEL || "gpt-4o";
const chatTemperature = Number.parseFloat(
  process.env.CHAT_TEMPERATURE || "0.2"
);
const chatMaxTokens = Number.parseInt(
  process.env.CHAT_MAX_TOKENS || "800",
  10
);
const chatTopK = Number.parseInt(process.env.CHAT_TOP_K || "8", 10);

const openai = openaiApiKey ? new OpenAI({ apiKey: openaiApiKey }) : null;

const githubApiBase = "https://api.github.com";
const githubToken = process.env.GITHUB_API_TOKEN || process.env.GITHUB_TOKEN;
const githubAppId = process.env.GITHUB_APP_ID;
const githubAppPrivateKeyPath = process.env.GITHUB_APP_PRIVATE_KEY_PATH;
const githubAppPrivateKeyRaw = process.env.GITHUB_APP_PRIVATE_KEY;
const githubAppInstallationId = process.env.GITHUB_APP_INSTALLATION_ID;

const resolvePrivateKey = () => {
  if (githubAppPrivateKeyRaw) {
    return githubAppPrivateKeyRaw;
  }
  if (!githubAppPrivateKeyPath) {
    return null;
  }
  const resolvedPath = path.isAbsolute(githubAppPrivateKeyPath)
    ? githubAppPrivateKeyPath
    : path.join(repoRoot, githubAppPrivateKeyPath);
  try {
    return fsSync.readFileSync(resolvedPath, "utf8");
  } catch (err) {
    console.error("Failed to read GitHub App private key:", err.message);
    return null;
  }
};

const githubAppPrivateKey = resolvePrivateKey();

let cachedInstallationToken = null;
let cachedInstallationExpiresAt = 0;

const readProjects = async () => {
  try {
    const raw = await fs.readFile(projectsFile, "utf8");
    const parsed = YAML.parse(raw) || {};
    return Array.isArray(parsed.projects) ? parsed.projects : [];
  } catch (err) {
    if (err.code === "ENOENT") {
      return [];
    }
    throw err;
  }
};

const writeProjects = async (projects) => {
  const serialized = YAML.stringify({ projects });
  await fs.writeFile(projectsFile, serialized, "utf8");
};

const slugify = (value) =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9-_]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

const parseGitHubRepo = (repoUrl) => {
  let url;
  try {
    url = new URL(repoUrl);
  } catch {
    return null;
  }

  if (url.hostname !== "github.com" && url.hostname !== "www.github.com") {
    return null;
  }

  const parts = url.pathname
    .replace(/^\/+/, "")
    .replace(/\.git$/, "")
    .split("/")
    .filter(Boolean);

  if (parts.length < 2) {
    return null;
  }

  const [owner, repo] = parts;
  if (!owner || !repo) {
    return null;
  }

  return { owner, repo };
};

const parseRepoFilter = (value) => {
  if (!value || typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (trimmed.includes("github.com")) {
    return parseGitHubRepo(trimmed);
  }

  const normalized = trimmed.replace(/^\/+/, "").replace(/\.git$/, "");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length >= 2) {
    return { owner: parts[0], repo: parts[1] };
  }

  return null;
};

const base64UrlEncode = (input) =>
  Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");

const createAppJwt = () => {
  if (!githubAppId || !githubAppPrivateKey) {
    return null;
  }

  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iat: now - 60,
    exp: now + 9 * 60,
    iss: githubAppId
  };

  const headerEncoded = base64UrlEncode(JSON.stringify(header));
  const payloadEncoded = base64UrlEncode(JSON.stringify(payload));
  const data = `${headerEncoded}.${payloadEncoded}`;
  const key = githubAppPrivateKey.includes("\\n")
    ? githubAppPrivateKey.replace(/\\n/g, "\n")
    : githubAppPrivateKey;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(data);
  signer.end();
  const signature = signer.sign(key);

  return `${data}.${base64UrlEncode(signature)}`;
};

const getInstallationToken = async () => {
  const now = Date.now();
  if (
    cachedInstallationToken &&
    cachedInstallationExpiresAt > now + 60_000
  ) {
    return cachedInstallationToken;
  }

  if (!githubAppInstallationId) {
    return null;
  }

  const jwt = createAppJwt();
  if (!jwt) {
    return null;
  }

  const response = await fetch(
    `${githubApiBase}/app/installations/${githubAppInstallationId}/access_tokens`,
    {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${jwt}`,
        "User-Agent": "github-projects-homepage-ai-chat",
        "X-GitHub-Api-Version": "2022-11-28"
      }
    }
  );

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.message || "GitHub App auth failed");
  }

  cachedInstallationToken = payload.token;
  cachedInstallationExpiresAt = payload.expires_at
    ? new Date(payload.expires_at).getTime()
    : now + 55 * 60 * 1000;
  return cachedInstallationToken;
};

const getGitHubAuthHeader = async () => {
  if (githubToken) {
    return { Authorization: `Bearer ${githubToken}` };
  }

  if (githubAppId && githubAppPrivateKey && githubAppInstallationId) {
    const token = await getInstallationToken();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  return {};
};

const fetchGitHubJson = async (endpoint, options = {}) => {
  try {
    const authHeader = await getGitHubAuthHeader();
    const response = await fetch(`${githubApiBase}${endpoint}`, {
      ...options,
      headers: {
        Accept: "application/vnd.github+json",
        "User-Agent": "github-projects-homepage-ai-chat",
        "X-GitHub-Api-Version": "2022-11-28",
        ...authHeader,
        ...(options.headers || {})
      }
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const remaining = response.headers.get("x-ratelimit-remaining");
      return {
        error: payload.message || `GitHub API error (${response.status})`,
        status: response.status,
        isRateLimit: response.status === 403 && remaining === "0"
      };
    }

    return { data: payload, status: response.status };
  } catch (err) {
    return { error: err.message || "GitHub API request failed." };
  }
};

const fetchRepoMetadata = async (owner, repo) => {
  const result = await fetchGitHubJson(`/repos/${owner}/${repo}`);
  if (result.error) {
    if (result.status === 404) {
      return { error: "Repository not found or not accessible." };
    }
    if (result.isRateLimit) {
      return {
        error:
          "GitHub rate limit exceeded. Configure GITHUB_TOKEN or GitHub App credentials."
      };
    }
    if (result.status === 401) {
      return { error: "GitHub authentication failed." };
    }
    return { error: result.error };
  }

  return { data: result.data };
};

const fetchReadmePath = async (owner, repo) => {
  const result = await fetchGitHubJson(`/repos/${owner}/${repo}/readme`);
  if (result.error) {
    return null;
  }
  return typeof result.data?.path === "string" ? result.data.path : null;
};

const normalizeProjectInput = async (body) => {
  const repoUrl = body?.repo || body?.repoUrl || body?.url;
  if (!repoUrl || typeof repoUrl !== "string") {
    return { error: "repoUrl is required" };
  }

  const parsed = parseGitHubRepo(repoUrl.trim());
  if (!parsed) {
    return { error: "Invalid GitHub repo URL" };
  }

  const repoInfo = await fetchRepoMetadata(parsed.owner, parsed.repo);
  if (repoInfo.error) {
    return { error: repoInfo.error };
  }

  const canonicalRepo =
    repoInfo.data?.html_url ||
    `https://github.com/${parsed.owner}/${parsed.repo}`;
  const defaultName =
    repoInfo.data?.full_name || `${parsed.owner}/${parsed.repo}`;
  const name =
    typeof body?.name === "string" && body.name.trim()
      ? body.name.trim()
      : defaultName;
  const description =
    typeof body?.description === "string" && body.description.trim()
      ? body.description.trim()
      : repoInfo.data?.description || "";
  const tags = Array.isArray(body?.tags)
    ? body.tags.filter((tag) => typeof tag === "string" && tag.trim())
    : Array.isArray(repoInfo.data?.topics)
    ? repoInfo.data.topics
    : [];
  const featured = Boolean(body?.featured);
  let docs = Array.isArray(body?.docs)
    ? body.docs.filter((doc) => typeof doc === "string" && doc.trim())
    : [];

  if (docs.length === 0) {
    const readmePath = await fetchReadmePath(parsed.owner, parsed.repo);
    if (readmePath) {
      docs = [readmePath];
    }
  }

  const project = {
    id: slugify(`${parsed.owner}-${parsed.repo}`),
    name,
    repo: canonicalRepo,
    description,
    tags,
    featured
  };

  if (docs.length > 0) {
    project.docs = docs;
  }

  return { project };
};

const extractRows = (result) => {
  if (Array.isArray(result)) {
    return result;
  }
  if (result && Array.isArray(result.rows)) {
    return result.rows;
  }
  return [];
};

const retrieveChunks = async (question, repoFilter, limit) => {
  if (!openai) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const embeddingResponse = await openai.embeddings.create({
    model: embeddingModel,
    input: question
  });

  const embedding = embeddingResponse.data?.[0]?.embedding;
  if (!embedding) {
    throw new Error("Failed to embed question");
  }

  const vector = `[${embedding.join(",")}]`;
  let query = sql`
    select
      c.id,
      c.content,
      c.metadata,
      s.path,
      s.url,
      s.repo_owner,
      s.repo_name,
      s.ref,
      s.ref_type
    from ${chunks} c
    join ${sources} s on s.id = c.source_id
  `;

  if (repoFilter) {
    query = sql`${query} where s.repo_owner = ${repoFilter.owner}
      and s.repo_name = ${repoFilter.repo}`;
  }

  query = sql`${query}
    order by c.embedding <=> ${vector}::vector
    limit ${limit}
  `;

  const result = await db.execute(query);
  return extractRows(result);
};

const requireAdmin = (request, reply) => {
  if (!adminApiKey) {
    reply.code(500).send({ error: "ADMIN_API_KEY is not set" });
    return false;
  }

  const provided =
    request.headers["x-admin-key"] || request.headers["x-api-key"];
  if (provided !== adminApiKey) {
    reply.code(401).send({ error: "Unauthorized" });
    return false;
  }

  return true;
};

app.get("/healthz", async () => ({ ok: true }));

app.get("/projects", async () => {
  const projects = await readProjects();
  return { projects };
});

app.post("/projects", async (request, reply) => {
  const { project, error } = await normalizeProjectInput(request.body);
  if (error) {
    reply.code(400).send({ error });
    return;
  }

  const projects = await readProjects();
  const existing = projects.find(
    (item) =>
      item.repo?.toLowerCase() === project.repo.toLowerCase() ||
      item.id === project.id
  );

  if (existing) {
    reply.code(200).send({ status: "exists", project: existing });
    return;
  }

  projects.push(project);
  await writeProjects(projects);
  reply.code(201).send({ status: "created", project });
});

app.post("/chat", async (_request, reply) => {
  const body = _request.body || {};
  const acceptHeader = _request.headers.accept || "";
  const wantsStream = body.stream === true || acceptHeader.includes("text/event-stream");
  const question =
    typeof body.question === "string"
      ? body.question
      : typeof body.message === "string"
      ? body.message
      : typeof body.prompt === "string"
      ? body.prompt
      : "";

  if (!question.trim()) {
    reply.code(400).send({ error: "question is required" });
    return;
  }

  const repoFilterInput =
    body.repo || body.repoUrl || body.project || body.projectRepo;
  const repoFilter = parseRepoFilter(repoFilterInput);
  const limit = Number.isFinite(Number(body.topK))
    ? Math.min(Math.max(Number(body.topK), 1), 20)
    : Math.min(Math.max(chatTopK, 1), 20);

  try {
    if (wantsStream) {
      reply.hijack();
      const allowedOrigin = resolveCorsOrigin(_request);
      if (allowedOrigin) {
        reply.raw.setHeader("Access-Control-Allow-Origin", allowedOrigin);
        reply.raw.setHeader("Vary", "Origin");
      }
      reply.raw.setHeader("Content-Type", "text/event-stream");
      reply.raw.setHeader("Cache-Control", "no-cache");
      reply.raw.setHeader("Connection", "keep-alive");
      reply.raw.flushHeaders?.();

      const sendEvent = (type, data) => {
        reply.raw.write(`event: ${type}\n`);
        reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      const abortController = new AbortController();
      const onClose = () => abortController.abort();
      reply.raw.on("close", onClose);

      try {
        const rows = await retrieveChunks(question, repoFilter, limit);
        if (rows.length === 0) {
          sendEvent("meta", { citations: [], context: { count: 0 } });
          sendEvent("delta", {
            delta: "I don't know based on the indexed sources."
          });
          sendEvent("done", {});
          reply.raw.end();
          return;
        }

        const citations = rows.map((row, index) => ({
          index: index + 1,
          repo:
            row.repo_owner && row.repo_name
              ? `${row.repo_owner}/${row.repo_name}`
              : null,
          path: row.path || null,
          ref: row.ref || null,
          url: row.url || null
        }));

        const contextBlocks = rows.map((row, index) => {
          const repoLabel =
            row.repo_owner && row.repo_name
              ? `${row.repo_owner}/${row.repo_name}`
              : "unknown";
          const header = `[source:${index + 1}] repo=${repoLabel} path=${
            row.path || "unknown"
          } url=${row.url || "n/a"}`;
          return `${header}\n${row.content}`;
        });

        const systemPrompt = [
          "You answer questions about GitHub repositories using ONLY the provided context.",
          "If the answer is not in the context, say you don't know.",
          "Cite sources using [source:n] where n matches the context block."
        ].join(" ");

        const userPrompt = `Question: ${question}\n\nContext:\n${contextBlocks.join(
          "\n\n"
        )}`;

        sendEvent("meta", { citations, context: { count: rows.length } });

        const stream = await openai.chat.completions.create({
          model: chatModel,
          temperature: Number.isFinite(chatTemperature) ? chatTemperature : 0.2,
          max_tokens: Number.isFinite(chatMaxTokens) ? chatMaxTokens : 800,
          stream: true,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt }
          ]
        }, {
          signal: abortController.signal
        });

        for await (const chunk of stream) {
          const delta = chunk.choices?.[0]?.delta?.content;
          if (delta) {
            sendEvent("delta", { delta });
          }
        }

        sendEvent("done", {});
      } catch (err) {
        sendEvent("error", { error: err.message || "Chat failed" });
      } finally {
        reply.raw.end();
        reply.raw.off("close", onClose);
      }

      return;
    }

    const rows = await retrieveChunks(question, repoFilter, limit);
    if (rows.length === 0) {
      reply.send({
        answer: "I don't know based on the indexed sources.",
        citations: [],
        context: { count: 0 }
      });
      return;
    }

    const citations = rows.map((row, index) => ({
      index: index + 1,
      repo:
        row.repo_owner && row.repo_name
          ? `${row.repo_owner}/${row.repo_name}`
          : null,
      path: row.path || null,
      ref: row.ref || null,
      url: row.url || null
    }));

    const contextBlocks = rows.map((row, index) => {
      const repoLabel =
        row.repo_owner && row.repo_name
          ? `${row.repo_owner}/${row.repo_name}`
          : "unknown";
      const header = `[source:${index + 1}] repo=${repoLabel} path=${
        row.path || "unknown"
      } url=${row.url || "n/a"}`;
      return `${header}\n${row.content}`;
    });

    const systemPrompt = [
      "You answer questions about GitHub repositories using ONLY the provided context.",
      "If the answer is not in the context, say you don't know.",
      "Cite sources using [source:n] where n matches the context block."
    ].join(" ");

    const userPrompt = `Question: ${question}\n\nContext:\n${contextBlocks.join(
      "\n\n"
    )}`;

    const completion = await openai.chat.completions.create({
      model: chatModel,
      temperature: Number.isFinite(chatTemperature) ? chatTemperature : 0.2,
      max_tokens: Number.isFinite(chatMaxTokens) ? chatMaxTokens : 800,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt }
      ]
    });

    const answer = completion.choices?.[0]?.message?.content?.trim() || "";

    reply.send({
      answer,
      citations,
      context: { count: rows.length }
    });
  } catch (err) {
    reply.code(500).send({ error: err.message || "Chat failed" });
  }
});

app.post("/admin/reindex", async (request, reply) => {
  if (!requireAdmin(request, reply)) {
    return;
  }

  const projects = (await readProjects()).filter(
    (project) => project && project.repo
  );
  if (projects.length === 0) {
    reply.send({ status: "empty", count: 0 });
    return;
  }

  const now = new Date();
  const jobRows = projects.map((project) => ({
    projectRepo: project.repo,
    projectName: project.name || project.repo,
    status: "queued",
    createdAt: now
  }));

  let inserted;
  try {
    inserted = await db
      .insert(ingestJobs)
      .values(jobRows)
      .returning({
        id: ingestJobs.id,
        projectRepo: ingestJobs.projectRepo,
        projectName: ingestJobs.projectName
      });
  } catch (err) {
    reply.code(500).send({ error: err.message || "Failed to enqueue jobs" });
    return;
  }

  const enqueued = [];
  for (const jobRecord of inserted) {
    const job = await ingestQueue.add(JOB_TYPES.ingestRepoDocs, {
      ingestJobId: jobRecord.id,
      repo: jobRecord.projectRepo,
      name: jobRecord.projectName
    });
    enqueued.push({
      jobId: job.id,
      ingestJobId: jobRecord.id,
      repo: jobRecord.projectRepo
    });
  }

  reply.send({ status: "queued", count: enqueued.length, jobs: enqueued });
});

app.get("/admin/jobs", async (request, reply) => {
  if (!requireAdmin(request, reply)) {
    return;
  }

  const rawLimit = Number.parseInt(request.query?.limit, 10);
  const limit = Number.isFinite(rawLimit)
    ? Math.min(Math.max(rawLimit, 1), 200)
    : 50;

  const jobs = await db
    .select({
      id: ingestJobs.id,
      projectRepo: ingestJobs.projectRepo,
      projectName: ingestJobs.projectName,
      totalFiles: ingestJobs.totalFiles,
      totalBytes: ingestJobs.totalBytes,
      filesProcessed: ingestJobs.filesProcessed,
      chunksStored: ingestJobs.chunksStored,
      status: ingestJobs.status,
      error: ingestJobs.error,
      lastMessage: ingestJobs.lastMessage,
      createdAt: ingestJobs.createdAt,
      startedAt: ingestJobs.startedAt,
      finishedAt: ingestJobs.finishedAt,
      updatedAt: ingestJobs.updatedAt
    })
    .from(ingestJobs)
    .orderBy(desc(ingestJobs.id))
    .limit(limit);

  reply.send({ jobs });
});

const port = Number(process.env.API_PORT || process.env.PORT || 3001);
const host = process.env.HOST || "0.0.0.0";

const start = async () => {
  try {
    await app.listen({ port, host });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

start();
