import crypto from "node:crypto";
import fsSync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "bullmq";
import { and, eq, inArray } from "drizzle-orm";
import { Client as MinioClient } from "minio";
import OpenAI from "openai";
import {
  JOB_TYPES,
  QUEUE_NAMES,
  chunks,
  getRedisConnectionOptions,
  ingestJobs,
  sources
} from "@app/shared";
import { db, pool } from "./db/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..", "..");

const connection = getRedisConnectionOptions();

const openaiApiKey = process.env.OPENAI_API_KEY;
const embeddingModel =
  process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";
const openai = openaiApiKey ? new OpenAI({ apiKey: openaiApiKey }) : null;

const minioClient = new MinioClient({
  endPoint: process.env.MINIO_ENDPOINT || "localhost",
  port: Number.parseInt(process.env.MINIO_PORT || "9000", 10),
  useSSL: process.env.MINIO_USE_SSL === "true",
  accessKey: process.env.MINIO_ACCESS_KEY || "minio",
  secretKey: process.env.MINIO_SECRET_KEY || "minio123"
});
const artifactsBucket = process.env.MINIO_BUCKET_ARTIFACTS || "artifacts";
const defaultTenantId = process.env.DEFAULT_TENANT_ID || "default";

const maxFiles = Number.parseInt(process.env.INGEST_MAX_FILES || "300", 10);
const maxFileBytes = Number.parseInt(
  process.env.INGEST_MAX_FILE_BYTES || "200000",
  10
);
const maxTotalBytes = Number.parseInt(
  process.env.INGEST_MAX_TOTAL_BYTES || "5000000",
  10
);
const maxChunksPerFile = Number.parseInt(
  process.env.INGEST_MAX_CHUNKS_PER_FILE || "30",
  10
);
const progressInterval = Math.max(
  Number.parseInt(process.env.INGEST_PROGRESS_INTERVAL || "10", 10),
  1
);
const chunkSize = Number.parseInt(process.env.INGEST_CHUNK_SIZE || "1200", 10);
const chunkOverlap = Number.parseInt(
  process.env.INGEST_CHUNK_OVERLAP || "200",
  10
);

const githubApiBase = "https://api.github.com";
const githubToken = process.env.GITHUB_API_TOKEN || process.env.GITHUB_TOKEN;
const githubAppId = process.env.GITHUB_APP_ID;
const githubAppPrivateKeyPath = process.env.GITHUB_APP_PRIVATE_KEY_PATH;
const githubAppPrivateKeyRaw = process.env.GITHUB_APP_PRIVATE_KEY;
const githubAppInstallationId = process.env.GITHUB_APP_INSTALLATION_ID;

const installationTokenCache = new Map();
const installationIdCache = new Map();
const installationIdCacheTtlMs = 10 * 60 * 1000;

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

const getCachedInstallationToken = (installationId) => {
  if (!installationId) {
    return null;
  }
  const cached = installationTokenCache.get(String(installationId));
  if (!cached) {
    return null;
  }
  const now = Date.now();
  if (cached.expiresAt && cached.expiresAt > now + 60_000) {
    return cached.token;
  }
  installationTokenCache.delete(String(installationId));
  return null;
};

const setCachedInstallationToken = (installationId, token, expiresAt) => {
  if (!installationId || !token) {
    return;
  }
  installationTokenCache.set(String(installationId), {
    token,
    expiresAt
  });
};

const getCachedInstallationId = (owner, repo) => {
  if (!owner || !repo) {
    return null;
  }
  const key = `${owner}/${repo}`.toLowerCase();
  const cached = installationIdCache.get(key);
  if (!cached) {
    return null;
  }
  if (cached.expiresAt > Date.now()) {
    return cached.id;
  }
  installationIdCache.delete(key);
  return null;
};

const setCachedInstallationId = (owner, repo, installationId) => {
  if (!owner || !repo || !installationId) {
    return;
  }
  const key = `${owner}/${repo}`.toLowerCase();
  installationIdCache.set(key, {
    id: installationId,
    expiresAt: Date.now() + installationIdCacheTtlMs
  });
};

const textExtensions = new Set([
  ".md",
  ".markdown",
  ".mdx",
  ".txt",
  ".js",
  ".jsx",
  ".ts",
  ".tsx",
  ".mjs",
  ".cjs",
  ".json",
  ".yml",
  ".yaml",
  ".toml",
  ".ini",
  ".env",
  ".py",
  ".rb",
  ".go",
  ".rs",
  ".java",
  ".kt",
  ".kts",
  ".swift",
  ".php",
  ".cs",
  ".cpp",
  ".c",
  ".h",
  ".hpp",
  ".html",
  ".css",
  ".scss",
  ".less",
  ".sql",
  ".graphql",
  ".gql",
  ".proto",
  ".sh",
  ".bash",
  ".ps1",
  ".bat",
  ".cmd"
]);

const allowedNoExtension = new Set([
  "dockerfile",
  "makefile",
  "license",
  "readme",
  "readme.md"
]);

const skipPrefixes = [
  "node_modules/",
  ".git/",
  "dist/",
  "build/",
  "out/",
  ".next/",
  "coverage/",
  "vendor/",
  "bin/",
  "obj/",
  "target/",
  ".venv/",
  "venv/",
  ".idea/",
  ".vscode/"
];

const skipFileNames = new Set([
  "package-lock.json",
  "yarn.lock",
  "pnpm-lock.yaml",
  "pnpm-lock.yml",
  "npm-shrinkwrap.json"
]);

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

const fetchInstallationIdForRepo = async (owner, repo) => {
  if (!owner || !repo) {
    return null;
  }
  const cached = getCachedInstallationId(owner, repo);
  if (cached) {
    return cached;
  }
  const jwt = createAppJwt();
  if (!jwt) {
    return null;
  }
  const response = await fetch(
    `${githubApiBase}/repos/${owner}/${repo}/installation`,
    {
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
    return null;
  }
  const installationId = payload?.id;
  if (installationId) {
    setCachedInstallationId(owner, repo, installationId);
  }
  return installationId || null;
};

const getInstallationToken = async (options = {}) => {
  const owner = options.owner;
  const repo = options.repo;
  const installationId =
    options.installationId ||
    githubAppInstallationId ||
    (owner && repo ? await fetchInstallationIdForRepo(owner, repo) : null);
  if (!installationId) {
    return null;
  }

  const cached = getCachedInstallationToken(installationId);
  if (cached) {
    return cached;
  }

  const jwt = createAppJwt();
  if (!jwt) {
    return null;
  }

  const response = await fetch(
    `${githubApiBase}/app/installations/${installationId}/access_tokens`,
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

  const now = Date.now();
  const expiresAt = payload.expires_at
    ? new Date(payload.expires_at).getTime()
    : now + 55 * 60 * 1000;
  setCachedInstallationToken(installationId, payload.token, expiresAt);
  return payload.token;
};

const getGitHubAuthHeader = async (options = {}) => {
  if (githubToken) {
    return { Authorization: `Bearer ${githubToken}` };
  }

  if (githubAppId && githubAppPrivateKey) {
    const token = await getInstallationToken(options);
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  return {};
};

const fetchGitHubJson = async (endpoint, options = {}) => {
  try {
    const { auth, ...fetchOptions } = options || {};
    const authHeader = await getGitHubAuthHeader(auth);
    const response = await fetch(`${githubApiBase}${endpoint}`, {
      ...fetchOptions,
      headers: {
        Accept: "application/vnd.github+json",
        "User-Agent": "github-projects-homepage-ai-chat",
        "X-GitHub-Api-Version": "2022-11-28",
        ...authHeader,
        ...(fetchOptions.headers || {})
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

    return { data: payload };
  } catch (err) {
    return { error: err.message || "GitHub API request failed." };
  }
};

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

const ensureBucket = async (bucket) => {
  const exists = await minioClient.bucketExists(bucket).catch(() => false);
  if (!exists) {
    await minioClient.makeBucket(bucket, "us-east-1");
  }
};

const isLikelyBinary = (buffer) => {
  if (!buffer || buffer.length === 0) {
    return false;
  }

  const sample = buffer.subarray(0, Math.min(buffer.length, 2000));
  let nonPrintable = 0;
  for (const byte of sample) {
    if (byte === 0) {
      return true;
    }
    if (byte < 9 || (byte > 13 && byte < 32)) {
      nonPrintable += 1;
    }
  }
  return nonPrintable / sample.length > 0.2;
};

const shouldIncludeFile = (filePath, size) => {
  if (!filePath) {
    return false;
  }

  const normalized = filePath.replace(/\\/g, "/").toLowerCase();
  if (skipPrefixes.some((prefix) => normalized.startsWith(prefix))) {
    return false;
  }

  const baseName = path.basename(normalized);
  if (skipFileNames.has(baseName)) {
    return false;
  }

  if (normalized.includes(".min.")) {
    return false;
  }

  const ext = path.extname(normalized);
  if (!ext) {
    return allowedNoExtension.has(baseName);
  }

  if (!textExtensions.has(ext)) {
    return false;
  }

  if (typeof size === "number" && size > maxFileBytes) {
    return false;
  }

  return true;
};

const chunkText = (text) => {
  const normalized = text.replace(/\r\n/g, "\n");
  const chunksList = [];
  const overlap = Math.max(Math.min(chunkOverlap, chunkSize - 1), 0);
  let start = 0;
  while (start < normalized.length) {
    const end = Math.min(start + chunkSize, normalized.length);
    const chunk = normalized.slice(start, end).trim();
    if (chunk) {
      chunksList.push(chunk);
    }
    if (end >= normalized.length) {
      break;
    }
    start = end - overlap;
  }
  return chunksList;
};

const embedChunks = async (chunkTexts) => {
  if (!openai) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const response = await openai.embeddings.create({
    model: embeddingModel,
    input: chunkTexts
  });
  return response.data.map((item) => item.embedding);
};

const fetchRepoMetadata = async (owner, repo) => {
  const result = await fetchGitHubJson(`/repos/${owner}/${repo}`, {
    auth: { owner, repo }
  });
  if (result.error) {
    if (result.isRateLimit) {
      throw new Error(
        "GitHub rate limit exceeded. Configure GITHUB_TOKEN or GitHub App credentials."
      );
    }
    throw new Error(result.error);
  }
  return result.data;
};

const fetchRepoTree = async (owner, repo, ref) => {
  const result = await fetchGitHubJson(
    `/repos/${owner}/${repo}/git/trees/${encodeURIComponent(ref)}?recursive=1`,
    { auth: { owner, repo } }
  );
  if (result.error) {
    throw new Error(result.error);
  }
  return result.data;
};

const fetchBlob = async (owner, repo, sha) => {
  const result = await fetchGitHubJson(
    `/repos/${owner}/${repo}/git/blobs/${sha}`,
    { auth: { owner, repo } }
  );
  if (result.error) {
    throw new Error(result.error);
  }
  return result.data;
};

const buildObjectKey = (tenantId, owner, repo, ref, filePath) => {
  const normalizedPath = filePath.replace(/\\/g, "/");
  const tenantKey = tenantId || defaultTenantId;
  return `tenants/${tenantKey}/repos/${owner}/${repo}/refs/${ref}/files/${normalizedPath}`;
};

const purgeExistingSources = async ({ projectId, owner, repo }) => {
  if (!projectId && (!owner || !repo)) {
    return;
  }
  const predicate = projectId
    ? eq(sources.projectId, projectId)
    : and(eq(sources.repoOwner, owner), eq(sources.repoName, repo));
  const existing = await db
    .select({ id: sources.id })
    .from(sources)
    .where(predicate);

  if (existing.length === 0) {
    return;
  }

  const ids = existing.map((row) => row.id);
  await db.delete(chunks).where(inArray(chunks.sourceId, ids));
  await db.delete(sources).where(inArray(sources.id, ids));
};

const updateJob = async (ingestJobId, values) => {
  if (!ingestJobId) {
    return;
  }
  await db
    .update(ingestJobs)
    .set({ ...values, updatedAt: new Date() })
    .where(eq(ingestJobs.id, ingestJobId));
};

const fetchJobStatus = async (ingestJobId) => {
  if (!ingestJobId) {
    return null;
  }
  const rows = await db
    .select({ status: ingestJobs.status })
    .from(ingestJobs)
    .where(eq(ingestJobs.id, ingestJobId))
    .limit(1);
  return rows[0]?.status || null;
};

const cancelIfRequested = async (ingestJobId) => {
  if (!ingestJobId) {
    return false;
  }
  const status = await fetchJobStatus(ingestJobId);
  if (status !== "cancel_requested" && status !== "canceled") {
    return false;
  }
  await updateJob(ingestJobId, {
    status: "canceled",
    finishedAt: new Date(),
    lastMessage: "Canceled by request"
  });
  return true;
};

const ingestRepo = async ({ repoUrl, ingestJobId, projectId, tenantId }) => {
  const parsed = parseGitHubRepo(repoUrl);
  if (!parsed) {
    throw new Error("Invalid repo URL");
  }

  console.log(`[worker] Starting ingest for ${parsed.owner}/${parsed.repo}`);
  if (await cancelIfRequested(ingestJobId)) {
    return { canceled: true };
  }
  await ensureBucket(artifactsBucket);

  const repoInfo = await fetchRepoMetadata(parsed.owner, parsed.repo);
  const defaultBranch = repoInfo.default_branch || "main";
  const tree = await fetchRepoTree(parsed.owner, parsed.repo, defaultBranch);
  const treeItems = Array.isArray(tree.tree) ? tree.tree : [];

  await purgeExistingSources({
    projectId,
    owner: parsed.owner,
    repo: parsed.repo
  });

  const selected = [];
  let totalBytes = 0;
  for (const item of treeItems) {
    if (await cancelIfRequested(ingestJobId)) {
      return { canceled: true };
    }
    if (item.type !== "blob") {
      continue;
    }
    if (!shouldIncludeFile(item.path, item.size)) {
      continue;
    }
    if (typeof item.size === "number") {
      if (totalBytes + item.size > maxTotalBytes) {
        break;
      }
      totalBytes += item.size;
    }
    selected.push(item);
    if (selected.length >= maxFiles) {
      break;
    }
  }

  if (selected.length === 0) {
    throw new Error("No eligible files found to ingest.");
  }

  await updateJob(ingestJobId, {
    totalFiles: selected.length,
    totalBytes,
    filesProcessed: 0,
    chunksStored: 0,
    lastMessage: `Selected ${selected.length} files`
  });
  console.log(
    `[worker] ${parsed.owner}/${parsed.repo}: selected ${selected.length} files (${totalBytes} bytes)`
  );

  let filesProcessed = 0;
  let chunksStored = 0;

  for (const file of selected) {
    if (await cancelIfRequested(ingestJobId)) {
      return { canceled: true };
    }
    const blob = await fetchBlob(parsed.owner, parsed.repo, file.sha);
    const buffer = Buffer.from(blob.content || "", "base64");
    if (isLikelyBinary(buffer)) {
      continue;
    }

    const text = buffer.toString("utf8");
    if (!text.trim()) {
      continue;
    }

    const chunksList = chunkText(text).slice(0, maxChunksPerFile);
    if (chunksList.length === 0) {
      continue;
    }

    const embeddings = await embedChunks(chunksList);

    const objectKey = buildObjectKey(
      tenantId,
      parsed.owner,
      parsed.repo,
      defaultBranch,
      file.path
    );

    await minioClient.putObject(artifactsBucket, objectKey, text, {
      "Content-Type": "text/plain"
    });

    const [sourceRow] = await db
      .insert(sources)
      .values({
        projectId: projectId || null,
        repoOwner: parsed.owner,
        repoName: parsed.repo,
        refType: "branch",
        ref: defaultBranch,
        path: file.path,
        url: `https://github.com/${parsed.owner}/${parsed.repo}/blob/${defaultBranch}/${file.path}`
      })
      .returning({ id: sources.id });

    const chunkRows = chunksList.map((chunk, index) => ({
      sourceId: sourceRow.id,
      content: chunk,
      embedding: embeddings[index],
      metadata: {
        repo: parsed.repo,
        owner: parsed.owner,
        ref: defaultBranch,
        path: file.path,
        chunkIndex: index
      }
    }));

    await db.insert(chunks).values(chunkRows);
    filesProcessed += 1;
    chunksStored += chunkRows.length;

    if (filesProcessed % progressInterval === 0) {
      await updateJob(ingestJobId, {
        filesProcessed,
        chunksStored,
        lastMessage: `Processed ${filesProcessed}/${selected.length} files`
      });
      console.log(
        `[worker] ${parsed.owner}/${parsed.repo}: ${filesProcessed}/${selected.length} files, ${chunksStored} chunks`
      );
    }
  }

  await updateJob(ingestJobId, {
    filesProcessed,
    chunksStored,
    lastMessage: `Completed ${filesProcessed} files`
  });
  console.log(
    `[worker] Completed ${parsed.owner}/${parsed.repo}: ${filesProcessed} files, ${chunksStored} chunks`
  );

  return { filesProcessed, chunksStored };
};

const ingestWorker = new Worker(
  QUEUE_NAMES.ingest,
  async (job) => {
    if (job.name !== JOB_TYPES.ingestRepoDocs) {
      return;
    }

    const { ingestJobId, repo, projectId, tenantId } = job.data || {};
    if (ingestJobId) {
      if (await cancelIfRequested(ingestJobId)) {
        return { canceled: true };
      }
      await updateJob(ingestJobId, {
        status: "running",
        startedAt: new Date(),
        lastMessage: "Starting ingest"
      });
    }

    try {
      const result = await ingestRepo({
        repoUrl: repo,
        ingestJobId,
        projectId,
        tenantId
      });

      if (ingestJobId) {
        if (result?.canceled) {
          await updateJob(ingestJobId, {
            status: "canceled",
            finishedAt: new Date(),
            lastMessage: "Canceled by request"
          });
          return { canceled: true };
        }
        await updateJob(ingestJobId, {
          status: "completed",
          finishedAt: new Date(),
          lastMessage: "Ingest completed"
        });
      }

      return { ok: true, ...result };
    } catch (err) {
      if (ingestJobId) {
        await updateJob(ingestJobId, {
          status: "failed",
          error: err.message || "Ingest failed",
          finishedAt: new Date(),
          lastMessage: "Ingest failed"
        });
      }

      throw err;
    }
  },
  { connection }
);

ingestWorker.on("failed", (job, err) => {
  console.error(`[worker] Job failed: ${job?.id}`, err);
});

const shutdown = async () => {
  await ingestWorker.close();
  await pool.end();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
