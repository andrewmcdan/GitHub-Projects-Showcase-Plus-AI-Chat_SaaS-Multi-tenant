import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import cors from "@fastify/cors";
import Fastify from "fastify";
import YAML from "yaml";

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

const githubApiBase = "https://api.github.com";
const githubToken = process.env.GITHUB_API_TOKEN || process.env.GITHUB_TOKEN;
const githubAppId = process.env.GITHUB_APP_ID;
const githubAppPrivateKey = process.env.GITHUB_APP_PRIVATE_KEY;
const githubAppInstallationId = process.env.GITHUB_APP_INSTALLATION_ID;

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
  reply.code(501).send({ error: "Not implemented" });
});

app.post("/admin/reindex", async (_request, reply) => {
  reply.code(501).send({ error: "Not implemented" });
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
