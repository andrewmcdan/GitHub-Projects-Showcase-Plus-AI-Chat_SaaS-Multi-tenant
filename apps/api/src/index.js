import crypto from "node:crypto";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Queue } from "bullmq";
import { and, desc, eq, sql } from "drizzle-orm";
import cors from "@fastify/cors";
import Fastify from "fastify";
import OpenAI from "openai";
import YAML from "yaml";
import {
    JOB_TYPES,
    QUEUE_NAMES,
    chatMessages,
    chatSessions,
    chunks,
    getRedisConnectionOptions,
    ingestJobs,
    sources,
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
    connection: getRedisConnectionOptions(),
});

const openaiApiKey = process.env.OPENAI_API_KEY;
const embeddingModel =
    process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";
const chatModel = process.env.OPENAI_CHAT_MODEL || "gpt-4o";
const chatTemperature = Number.parseFloat(
    process.env.CHAT_TEMPERATURE || "0.2"
);
const chatMaxTokens = Number.parseInt(process.env.CHAT_MAX_TOKENS || "800", 10);
const chatTopK = Number.parseInt(process.env.CHAT_TOP_K || "12", 10);
const chatHistoryLimit = Number.parseInt(
    process.env.CHAT_HISTORY_LIMIT || "8",
    10
);
const chatNeighborChunksRaw = Number.parseInt(
    process.env.CHAT_NEIGHBOR_CHUNKS || "1",
    10
);
const chatNeighborChunks =
    Number.isFinite(chatNeighborChunksRaw) && chatNeighborChunksRaw > 0
        ? chatNeighborChunksRaw
        : 0;
const chatMaxContextChunksRaw = Number.parseInt(
    process.env.CHAT_MAX_CONTEXT_CHUNKS || "",
    10
);
const chatMaxContextChunks =
    Number.isFinite(chatMaxContextChunksRaw) && chatMaxContextChunksRaw > 0
        ? chatMaxContextChunksRaw
        : Math.min(
              Math.max(
                  chatTopK * (chatNeighborChunks * 2 + 1),
                  chatTopK
              ),
              40
          );
const chatSnippetMaxChunks = Number.parseInt(
    process.env.CHAT_SNIPPET_MAX_CHUNKS || "4",
    10
);
const chatSnippetMaxChars = Number.parseInt(
    process.env.CHAT_SNIPPET_MAX_CHARS || "4000",
    10
);
const chatSnippetMaxLines = Number.parseInt(
    process.env.CHAT_SNIPPET_MAX_LINES || "160",
    10
);
const chatSessionTtlDays = Number.parseInt(
    process.env.CHAT_SESSION_TTL_DAYS || "90",
    10
);
const chatSessionTtlMs =
    Number.isFinite(chatSessionTtlDays) && chatSessionTtlDays > 0
        ? chatSessionTtlDays * 24 * 60 * 60 * 1000
        : 0;
const chatSessionCleanupIntervalMinutesRaw = Number.parseInt(
    process.env.CHAT_SESSION_CLEANUP_INTERVAL_MINUTES || "60",
    10
);
const chatSessionCleanupIntervalMinutes =
    Number.isFinite(chatSessionCleanupIntervalMinutesRaw) &&
    chatSessionCleanupIntervalMinutesRaw > 0
        ? Math.max(chatSessionCleanupIntervalMinutesRaw, 5)
        : 0;
const chatSessionCleanupIntervalMs =
    chatSessionCleanupIntervalMinutes > 0
        ? chatSessionCleanupIntervalMinutes * 60 * 1000
        : 0;
let lastChatCleanupAt = 0;

const ingestReindexIntervalMinutesRaw = Number.parseInt(
    process.env.INGEST_REINDEX_INTERVAL_MINUTES || "60",
    10
);
const ingestReindexIntervalMinutes =
    Number.isFinite(ingestReindexIntervalMinutesRaw) &&
    ingestReindexIntervalMinutesRaw > 0
        ? ingestReindexIntervalMinutesRaw
        : 0;
const ingestReindexIntervalMs =
    ingestReindexIntervalMinutes > 0
        ? ingestReindexIntervalMinutes * 60 * 1000
        : 0;
const ingestInitialCheckMinutesRaw = Number.parseInt(
    process.env.INGEST_INITIAL_INDEX_CHECK_MINUTES || "10",
    10
);
const ingestInitialCheckMinutes =
    Number.isFinite(ingestInitialCheckMinutesRaw) &&
    ingestInitialCheckMinutesRaw > 0
        ? ingestInitialCheckMinutesRaw
        : 0;
const ingestInitialCheckMs =
    ingestInitialCheckMinutes > 0
        ? ingestInitialCheckMinutes * 60 * 1000
        : 0;
const ingestQueueStaleMinutesRaw = Number.parseInt(
    process.env.INGEST_QUEUE_STALE_MINUTES || "10",
    10
);
const ingestQueueStaleMinutes =
    Number.isFinite(ingestQueueStaleMinutesRaw) &&
    ingestQueueStaleMinutesRaw > 0
        ? ingestQueueStaleMinutesRaw
        : 0;
const ingestQueueStaleMs =
    ingestQueueStaleMinutes > 0
        ? ingestQueueStaleMinutes * 60 * 1000
        : 0;
const activeIngestStatuses = new Set([
    "queued",
    "running",
    "cancel_requested",
]);
let ingestScheduleInFlight = false;

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

const normalizeLoose = (value) =>
    value.toLowerCase().replace(/[^a-z0-9]+/g, "");

const tokenizeText = (value) =>
    value.toLowerCase().split(/[^a-z0-9]+/g).filter(Boolean);

const stopWords = new Set([
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "how",
    "i",
    "in",
    "is",
    "it",
    "its",
    "me",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "you",
    "your",
]);

const repoIntentTokens = new Set([
    "project",
    "repo",
    "repository",
    "codebase",
    "app",
    "service",
    "library",
]);

const hasProjectIntent = (question) => {
    if (typeof question !== "string") {
        return false;
    }
    const normalized = question.trim().toLowerCase();
    if (!normalized) {
        return false;
    }
    if (
        normalized.startsWith("what is ") ||
        normalized.startsWith("tell me about ") ||
        normalized.startsWith("describe ") ||
        normalized.startsWith("explain ") ||
        normalized.startsWith("summarize ") ||
        normalized.startsWith("overview of ")
    ) {
        return true;
    }
    const tokens = tokenizeText(question);
    return tokens.some((token) => repoIntentTokens.has(token));
};

const hasTokenSequence = (tokens, phraseTokens, minTokens) => {
    if (!Array.isArray(tokens) || !Array.isArray(phraseTokens)) {
        return false;
    }
    if (tokens.length === 0 || phraseTokens.length === 0) {
        return false;
    }
    const minLen = Math.min(
        phraseTokens.length,
        Math.max(minTokens || phraseTokens.length, 1)
    );
    const haystack = ` ${tokens.join(" ")} `;
    for (let len = phraseTokens.length; len >= minLen; len -= 1) {
        for (let start = 0; start <= phraseTokens.length - len; start += 1) {
            const needle = ` ${phraseTokens
                .slice(start, start + len)
                .join(" ")} `;
            if (haystack.includes(needle)) {
                return true;
            }
        }
    }
    return false;
};

const isExplicitTokenMatch = (questionTokens, phraseTokens, hasIntent) => {
    if (!Array.isArray(phraseTokens) || phraseTokens.length === 0) {
        return false;
    }
    const minTokens = phraseTokens.length >= 2 ? 2 : 1;
    if (!hasTokenSequence(questionTokens, phraseTokens, minTokens)) {
        return false;
    }
    if (phraseTokens.length === 1) {
        const token = phraseTokens[0];
        if (token.length < 5 && !hasIntent) {
            return false;
        }
    }
    return true;
};

const extractKeywords = (value) => {
    if (typeof value !== "string") {
        return [];
    }
    const tokens = tokenizeText(value);
    const filtered = tokens.filter(
        (token) => token.length >= 3 && !stopWords.has(token)
    );
    return [...new Set(filtered)].slice(0, 8);
};

const isStatsQuestion = (question) => {
    if (typeof question !== "string") {
        return false;
    }
    const normalized = question.toLowerCase();
    return (
        normalized.includes("most code") ||
        normalized.includes("largest code") ||
        normalized.includes("largest codebase") ||
        normalized.includes("biggest code") ||
        normalized.includes("most lines") ||
        normalized.includes("most files") ||
        normalized.includes("largest project") ||
        normalized.includes("biggest project")
    );
};

const isEntryPointQuestion = (question) => {
    if (typeof question !== "string") {
        return false;
    }
    const normalized = question.toLowerCase();
    return (
        normalized.includes("entry point") ||
        normalized.includes("entrypoint") ||
        normalized.includes("startup") ||
        normalized.includes("bootstrap") ||
        normalized.includes("starting point")
    );
};

const entrypointConfigFiles = new Set([
    "package.json",
    "pyproject.toml",
    "setup.py",
    "cargo.toml",
    "go.mod",
    "cmakelists.txt",
    "makefile",
    "build.gradle",
    "pom.xml",
]);

const entrypointFileStems = new Set([
    "main",
    "index",
    "app",
    "server",
    "cli",
    "program",
    "__main__",
]);

const scoreEntryPointPath = (pathValue) => {
    if (!pathValue) {
        return -1;
    }
    const normalized = pathValue.replace(/\\/g, "/").toLowerCase();
    const baseName = path.basename(normalized);
    const ext = path.extname(baseName);
    const stem = ext ? baseName.slice(0, -ext.length) : baseName;
    let score = 0;

    if (entrypointConfigFiles.has(baseName)) {
        score = Math.max(score, 2);
    }
    if (entrypointFileStems.has(stem)) {
        score = Math.max(score, 4);
    }
    if (normalized.includes("/src/")) {
        score += 0.5;
    }
    if (
        normalized.includes("/test/") ||
        normalized.includes("/tests/") ||
        normalized.includes("/__tests__/")
    ) {
        score -= 1.5;
    }

    return score;
};

const selectEntryPointRow = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) {
        return null;
    }

    const candidates = rows
        .map((row) => ({
            row,
            score: scoreEntryPointPath(row.path),
            pathLength: row.path ? row.path.length : 9999,
        }))
        .filter((item) => item.score > 0)
        .sort((a, b) => {
            if (b.score !== a.score) {
                return b.score - a.score;
            }
            return a.pathLength - b.pathLength;
        });

    return candidates[0]?.row || null;
};

const guessCodeFenceLanguage = (pathValue) => {
    if (!pathValue) {
        return "";
    }
    const ext = path.extname(pathValue.toLowerCase());
    const map = {
        ".js": "javascript",
        ".jsx": "jsx",
        ".ts": "ts",
        ".tsx": "tsx",
        ".py": "python",
        ".go": "go",
        ".rs": "rust",
        ".cpp": "cpp",
        ".c": "c",
        ".h": "c",
        ".hpp": "cpp",
        ".java": "java",
        ".kt": "kotlin",
        ".cs": "csharp",
        ".rb": "ruby",
        ".php": "php",
        ".swift": "swift",
        ".json": "json",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".toml": "toml",
        ".md": "markdown",
        ".sh": "bash",
        ".ps1": "powershell",
        ".bat": "batch",
        ".cmd": "batch",
        ".vue": "vue",
    };
    return map[ext] || "";
};

const parseRepoFromProject = (project) => {
    if (!project || typeof project !== "object") {
        return null;
    }
    const repoValue =
        typeof project.repo === "string" ? project.repo.trim() : "";
    if (repoValue) {
        const parsed = parseRepoFilter(repoValue);
        if (parsed) {
            return parsed;
        }
    }
    const nameValue =
        typeof project.name === "string" ? project.name.trim() : "";
    if (nameValue) {
        const parsed = parseRepoFilter(nameValue);
        if (parsed) {
            return parsed;
        }
    }
    return null;
};

const resolveRepoFromQuestion = async (question) => {
    if (typeof question !== "string" || !question.trim()) {
        return null;
    }

    const projects = await readProjects();
    if (projects.length === 0) {
        return null;
    }

    const normalizedQuestion = question.toLowerCase();
    const looseQuestion = normalizeLoose(question);
    const questionTokens = tokenizeText(question);
    const questionTokenSet = new Set(questionTokens);
    const questionHasIntent = hasProjectIntent(question);
    const candidates = [];

    for (const project of projects) {
        const repo = parseRepoFromProject(project);
        if (!repo) {
            continue;
        }
        const repoId = `${repo.owner}/${repo.repo}`;
        const repoLower = repoId.toLowerCase();
        if (normalizedQuestion.includes(repoLower)) {
            candidates.push({ repo, score: 3, explicit: true });
            continue;
        }

        const nameValue =
            typeof project.name === "string" ? project.name.trim() : "";
        const nameLoose = normalizeLoose(nameValue);
        const nameTokens = tokenizeText(nameValue);
        const explicitNameMatch = isExplicitTokenMatch(
            questionTokens,
            nameTokens,
            questionHasIntent
        );
        if (nameLoose && looseQuestion.includes(nameLoose)) {
            candidates.push({
                repo,
                score: 2,
                explicit: explicitNameMatch,
            });
            continue;
        }

        const repoName = repo.repo || "";
        const repoLoose = normalizeLoose(repoName);
        const repoTokens = tokenizeText(repoName);
        const explicitRepoMatch = isExplicitTokenMatch(
            questionTokens,
            repoTokens,
            questionHasIntent
        );
        if (repoLoose && looseQuestion.includes(repoLoose)) {
            candidates.push({
                repo,
                score: 1,
                explicit: explicitRepoMatch,
            });
            continue;
        }

        const descriptionValue =
            typeof project.description === "string"
                ? project.description.trim()
                : "";
        const descriptionTokens = tokenizeText(descriptionValue);
        const tagTokens = Array.isArray(project.tags)
            ? project.tags.flatMap((tag) =>
                  typeof tag === "string" ? tokenizeText(tag) : []
              )
            : [];
        const metaTokens = new Set([
            ...tokenizeText(nameValue),
            ...descriptionTokens,
            ...tagTokens,
        ]);
        const overlap = [...metaTokens].filter(
            (token) => token.length >= 4 && questionTokenSet.has(token)
        );
        if (overlap.length >= 2) {
            candidates.push({ repo, score: 1, explicit: false });
            continue;
        }
        if (overlap.length === 1) {
            candidates.push({ repo, score: 0.5, explicit: false });
            continue;
        }

        const tokenMatches = [
            ...new Set([...tokenizeText(nameValue), ...tokenizeText(repoId)]),
        ].filter(
            (token) => token.length >= 4 && questionTokenSet.has(token)
        );
        if (tokenMatches.length > 0) {
            candidates.push({ repo, score: 0, explicit: false });
        }
    }

    if (candidates.length === 0) {
        return null;
    }

    candidates.sort((a, b) => b.score - a.score);
    const topScore = candidates[0].score;
    const topRepos = [
        ...new Map(
            candidates
                .filter((item) => item.score === topScore)
                .map((item) => [`${item.repo.owner}/${item.repo.repo}`, item])
        ).values(),
    ];

    return topRepos.length === 1 ? topRepos[0] : null;
};

const isSameRepo = (left, right) => {
    if (!left || !right) {
        return false;
    }
    return (
        left.owner.toLowerCase() === right.owner.toLowerCase() &&
        left.repo.toLowerCase() === right.repo.toLowerCase()
    );
};

const normalizeChatHistory = (value, limit) => {
    if (!Array.isArray(value)) {
        return [];
    }

    const cleaned = value
        .map((item) => {
            if (!item || typeof item !== "object") {
                return null;
            }
            const role = item.role === "assistant" ? "assistant" : item.role === "user" ? "user" : null;
            const content =
                typeof item.content === "string" ? item.content.trim() : "";
            if (!role || !content) {
                return null;
            }
            const citations =
                Array.isArray(item.citations) && item.citations.length > 0
                    ? item.citations
                    : null;
            return citations ? { role, content, citations } : { role, content };
        })
        .filter(Boolean);

    if (!Number.isFinite(limit) || limit <= 0) {
        return cleaned;
    }

    return cleaned.length > limit ? cleaned.slice(-limit) : cleaned;
};

const historyToText = (history) =>
    history
        .map(
            (item) =>
                `${item.role === "assistant" ? "Assistant" : "User"}: ${
                    item.content
                }`
        )
        .join("\n");

const buildRetrievalQuestion = (question, history) => {
    const lastUser = [...history]
        .reverse()
        .find((item) => item.role === "user");
    if (!lastUser) {
        return question;
    }
    return `${lastUser.content}\n\nFollow-up: ${question}`;
};

const inferRepoFromHistory = (history) => {
    for (let index = history.length - 1; index >= 0; index -= 1) {
        const item = history[index];
        const citations = item?.citations;
        if (!Array.isArray(citations) || citations.length === 0) {
            continue;
        }
        const counts = new Map();
        for (const citation of citations) {
            const repo =
                citation && typeof citation.repo === "string"
                    ? citation.repo.trim()
                    : "";
            if (!repo) {
                continue;
            }
            counts.set(repo, (counts.get(repo) || 0) + 1);
        }
        if (counts.size === 0) {
            continue;
        }
        const [repo] = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];
        return parseRepoFilter(repo);
    }

    return null;
};

const normalizeVisitorId = (value) => {
    if (typeof value !== "string") {
        return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }
    return trimmed.slice(0, 200);
};

const normalizeSessionId = (value) => {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return null;
    }
    return parsed;
};

const fetchChatSession = async (sessionId) => {
    if (!sessionId) {
        return null;
    }
    const rows = await db
        .select({
            id: chatSessions.id,
            visitorId: chatSessions.visitorId,
            createdAt: chatSessions.createdAt,
        })
        .from(chatSessions)
        .where(eq(chatSessions.id, sessionId))
        .limit(1);
    return rows[0] || null;
};

const createChatSession = async (visitorId) => {
    const result = await db
        .insert(chatSessions)
        .values({ visitorId: visitorId || null })
        .returning({ id: chatSessions.id });
    return result[0]?.id || null;
};

const fetchChatHistory = async (sessionId, limit) => {
    if (!sessionId) {
        return [];
    }
    const cappedLimit =
        Number.isFinite(limit) && limit > 0 ? Math.min(limit, 200) : 50;
    const rows = await db
        .select({
            id: chatMessages.id,
            role: chatMessages.role,
            content: chatMessages.content,
            citations: chatMessages.citations,
            createdAt: chatMessages.createdAt,
        })
        .from(chatMessages)
        .where(eq(chatMessages.sessionId, sessionId))
        .orderBy(desc(chatMessages.id))
        .limit(cappedLimit);
    return rows.reverse();
};

const storeChatMessage = async ({
    sessionId,
    role,
    content,
    citations = null,
}) => {
    if (!sessionId || !role || !content) {
        return;
    }
    await db.insert(chatMessages).values({
        sessionId,
        role,
        content,
        citations,
    });
};

const listChatSessions = async (visitorId, limit) => {
    const cappedLimit =
        Number.isFinite(limit) && limit > 0 ? Math.min(limit, 200) : 50;
    const result = await db.execute(sql`
        select
            s.id as "id",
            s.created_at as "createdAt",
            m.content as "lastMessage",
            m.role as "lastRole",
            m.created_at as "lastMessageAt"
        from ${chatSessions} s
        left join lateral (
            select content, role, created_at
            from ${chatMessages} m
            where m.session_id = s.id
            order by m.id desc
            limit 1
        ) m on true
        where s.visitor_id = ${visitorId}
        order by coalesce(m.created_at, s.created_at) desc
        limit ${cappedLimit}
    `);
    return extractRows(result);
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
        iss: githubAppId,
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
    if (cachedInstallationToken && cachedInstallationExpiresAt > now + 60_000) {
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
                "X-GitHub-Api-Version": "2022-11-28",
            },
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
                ...(options.headers || {}),
            },
        });

        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
            const remaining = response.headers.get("x-ratelimit-remaining");
            return {
                error:
                    payload.message || `GitHub API error (${response.status})`,
                status: response.status,
                isRateLimit: response.status === 403 && remaining === "0",
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
                error: "GitHub rate limit exceeded. Configure GITHUB_TOKEN or GitHub App credentials.",
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
        featured,
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

const fetchLatestIngestJob = async (projectRepo) => {
    if (!projectRepo) {
        return null;
    }
    const rows = await db
        .select({
            status: ingestJobs.status,
            createdAt: ingestJobs.createdAt,
            updatedAt: ingestJobs.updatedAt,
            finishedAt: ingestJobs.finishedAt,
        })
        .from(ingestJobs)
        .where(eq(ingestJobs.projectRepo, projectRepo))
        .orderBy(desc(ingestJobs.id))
        .limit(1);
    return rows[0] || null;
};

const getJobTimestamp = (job) => {
    if (!job) {
        return null;
    }
    const value = job.finishedAt || job.updatedAt || job.createdAt;
    const timestamp = new Date(value || 0).getTime();
    return Number.isFinite(timestamp) ? timestamp : null;
};

const hasSourcesForRepo = async (repo) => {
    if (!repo?.owner || !repo?.repo) {
        return false;
    }
    const rows = await db
        .select({ id: sources.id })
        .from(sources)
        .where(
            and(
                eq(sources.repoOwner, repo.owner),
                eq(sources.repoName, repo.repo)
            )
        )
        .limit(1);
    return rows.length > 0;
};

const enqueueReindexForAllProjects = async () => {
    const projects = (await readProjects()).filter(
        (project) => project && project.repo
    );
    if (projects.length === 0) {
        return { enqueued: 0 };
    }

    const now = Date.now();
    const toEnqueue = [];
    let skippedActive = 0;
    let skippedRecent = 0;

    for (const project of projects) {
        const latestJob = await fetchLatestIngestJob(project.repo);
        if (latestJob && activeIngestStatuses.has(latestJob.status)) {
            skippedActive += 1;
            continue;
        }
        if (ingestReindexIntervalMs && latestJob) {
            const timestamp = getJobTimestamp(latestJob);
            if (timestamp && now - timestamp < ingestReindexIntervalMs) {
                skippedRecent += 1;
                continue;
            }
        }
        toEnqueue.push(project);
    }

    if (toEnqueue.length === 0) {
        return { enqueued: 0, skippedActive, skippedRecent };
    }

    const enqueued = await enqueueIngestJobs(toEnqueue);
    return { enqueued: enqueued.length, skippedActive, skippedRecent };
};

const enqueueInitialIndexForMissingProjects = async () => {
    const projects = (await readProjects()).filter(
        (project) => project && project.repo
    );
    if (projects.length === 0) {
        return { enqueued: 0 };
    }

    const now = Date.now();
    const toEnqueue = [];
    let skippedActive = 0;
    let skippedRecent = 0;
    let skippedIndexed = 0;

    for (const project of projects) {
        const repo = parseRepoFromProject(project);
        if (!repo) {
            continue;
        }
        const hasSources = await hasSourcesForRepo(repo);
        if (hasSources) {
            skippedIndexed += 1;
            continue;
        }
        const latestJob = await fetchLatestIngestJob(project.repo);
        if (latestJob && activeIngestStatuses.has(latestJob.status)) {
            skippedActive += 1;
            continue;
        }
        if (ingestInitialCheckMs && latestJob) {
            const timestamp = getJobTimestamp(latestJob);
            if (timestamp && now - timestamp < ingestInitialCheckMs) {
                skippedRecent += 1;
                continue;
            }
        }
        toEnqueue.push(project);
    }

    if (toEnqueue.length === 0) {
        return {
            enqueued: 0,
            skippedActive,
            skippedRecent,
            skippedIndexed,
        };
    }

    const enqueued = await enqueueIngestJobs(toEnqueue);
    return {
        enqueued: enqueued.length,
        skippedActive,
        skippedRecent,
        skippedIndexed,
    };
};

const requeueStaleIngestJobs = async () => {
    if (!ingestQueueStaleMs) {
        return { requeued: 0, checked: 0 };
    }
    const cutoff = new Date(Date.now() - ingestQueueStaleMs);
    const rows = await db
        .select({
            id: ingestJobs.id,
            projectRepo: ingestJobs.projectRepo,
            projectName: ingestJobs.projectName,
        })
        .from(ingestJobs)
        .where(
            and(
                sql`${ingestJobs.updatedAt} < ${cutoff}`,
                sql`${ingestJobs.status} in ('queued','cancel_requested')`
            )
        )
        .orderBy(desc(ingestJobs.id))
        .limit(100);

    if (rows.length === 0) {
        return { requeued: 0, checked: 0 };
    }

    let requeued = 0;
    let canceled = 0;
    for (const row of rows) {
        const queueJob = await ingestQueue.getJob(`ingest-${row.id}`);
        if (!queueJob) {
            if (ingestQueueStaleMs) {
                await db
                    .update(ingestJobs)
                    .set({
                        status: "canceled",
                        finishedAt: new Date(),
                        lastMessage: "Canceled after stale request",
                        updatedAt: new Date(),
                    })
                    .where(eq(ingestJobs.id, row.id));
                canceled += 1;
            }
            continue;
        }
        try {
            const statusRows = await db
                .select({ status: ingestJobs.status })
                .from(ingestJobs)
                .where(eq(ingestJobs.id, row.id))
                .limit(1);
            const status = statusRows[0]?.status;
            if (status === "cancel_requested") {
                await ingestQueue.removeJobs(`ingest-${row.id}`);
                await db
                    .update(ingestJobs)
                    .set({
                        status: "canceled",
                        finishedAt: new Date(),
                        lastMessage: "Canceled by admin",
                        updatedAt: new Date(),
                    })
                    .where(eq(ingestJobs.id, row.id));
                canceled += 1;
            } else {
                await ingestQueue.add(
                    JOB_TYPES.ingestRepoDocs,
                    {
                        ingestJobId: row.id,
                        repo: row.projectRepo,
                        name: row.projectName || row.projectRepo,
                    },
                    { jobId: `ingest-${row.id}` }
                );
                requeued += 1;
                await db
                    .update(ingestJobs)
                    .set({
                        lastMessage: "Requeued stale job",
                        updatedAt: new Date(),
                    })
                    .where(eq(ingestJobs.id, row.id));
            }
        } catch (err) {
            app.log.warn(err);
        }
    }

    return { requeued, canceled, checked: rows.length };
};

const fetchExpiredChatSessionIds = async (cutoff) => {
    if (!cutoff) {
        return [];
    }
    const result = await db.execute(sql`
        select
            s.id as "id"
        from ${chatSessions} s
        left join ${chatMessages} m on m.session_id = s.id
        group by s.id, s.created_at
        having coalesce(max(m.created_at), s.created_at) < ${cutoff}
        order by s.id
    `);
    return extractRows(result)
        .map((row) => Number.parseInt(row.id, 10))
        .filter((id) => Number.isFinite(id));
};

const purgeExpiredChatSessions = async () => {
    if (!chatSessionTtlMs) {
        return { skipped: true, expired: 0 };
    }
    const cutoff = new Date(Date.now() - chatSessionTtlMs);
    const expiredIds = await fetchExpiredChatSessionIds(cutoff);
    if (expiredIds.length === 0) {
        return { expired: 0 };
    }

    const idSql = sql.join(
        expiredIds.map((id) => sql`${id}`),
        sql`, `
    );
    await db.execute(sql`
        delete from ${chatMessages}
        where session_id in (${idSql})
    `);
    await db.execute(sql`
        delete from ${chatSessions}
        where id in (${idSql})
    `);

    return { expired: expiredIds.length };
};

const maybePurgeExpiredChatSessions = async () => {
    if (!chatSessionTtlMs || !chatSessionCleanupIntervalMs) {
        return;
    }
    const now = Date.now();
    if (now - lastChatCleanupAt < chatSessionCleanupIntervalMs) {
        return;
    }
    lastChatCleanupAt = now;
    try {
        const result = await purgeExpiredChatSessions();
        if (result?.expired) {
            app.log.info(
                { expired: result.expired },
                "Purged expired chat sessions"
            );
        }
    } catch (err) {
        app.log.error(err);
    }
};

const retrieveChunks = async (question, repoFilter, limit) => {
    if (!openai) {
        throw new Error("OPENAI_API_KEY is not set");
    }

    const embeddingResponse = await openai.embeddings.create({
        model: embeddingModel,
        input: question,
    });

    const embedding = embeddingResponse.data?.[0]?.embedding;
    if (!embedding) {
        throw new Error("Failed to embed question");
    }

    const vector = `[${embedding.join(",")}]`;
    let query = sql`
    select
      c.id,
      c.source_id as "sourceId",
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

const retrieveLexicalChunks = async (keywords, repoFilter, limit) => {
    if (!Array.isArray(keywords) || keywords.length === 0) {
        return [];
    }

    const patterns = keywords.map((keyword) => `%${keyword}%`);
    const patternSql = sql`ARRAY[${sql.join(
        patterns.map((pattern) => sql`${pattern}`),
        sql`, `
    )}]`;
    const keywordClause = sql`(c.content ILIKE ANY (${patternSql}) or s.path ILIKE ANY (${patternSql}))`;

    let query = sql`
      select
        c.id,
        c.source_id as "sourceId",
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
          and s.repo_name = ${repoFilter.repo}
          and ${keywordClause}`;
    } else {
        query = sql`${query} where ${keywordClause}`;
    }

    query = sql`${query}
      order by c.id desc
      limit ${limit}
    `;

    const result = await db.execute(query);
    return extractRows(result);
};

const retrieveEntryPointChunks = async (repoFilter, limit) => {
    const patterns = [
        "%/main.%",
        "%/index.%",
        "%/app.%",
        "%/server.%",
        "%/cli.%",
        "%/program.%",
        "%/__main__.py%",
        "%/main.go%",
        "%/main.rs%",
        "%/main.cpp%",
        "%/main.c%",
        "%/main.java%",
        "%/main.kt%",
        "%/main.ts%",
        "%/main.js%",
        "%/main.tsx%",
        "%/main.jsx%",
        "%/main.vue%",
        "%/app.tsx%",
        "%/app.jsx%",
        "%/app.vue%",
        "%/package.json%",
        "%/pyproject.toml%",
        "%/setup.py%",
        "%/cargo.toml%",
        "%/go.mod%",
        "%/cmakelists.txt%",
        "%/makefile%",
        "%/gradle.build%",
        "%/build.gradle%",
        "%/pom.xml%"
    ];
    const patternSql = sql`ARRAY[${sql.join(
        patterns.map((pattern) => sql`${pattern}`),
        sql`, `
    )}]`;
    const pathClause = sql`s.path ILIKE ANY (${patternSql})`;

    let query = sql`
      select
        c.id,
        c.source_id as "sourceId",
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
      where ${pathClause}
        and (c.metadata->>'chunkIndex')::int in (0, 1)
    `;

    if (repoFilter) {
        query = sql`${query} and s.repo_owner = ${repoFilter.owner}
          and s.repo_name = ${repoFilter.repo}`;
    }

    query = sql`${query}
      order by s.path asc
      limit ${limit}
    `;

    const result = await db.execute(query);
    return extractRows(result);
};

const mergeRows = (primary, secondary, maxRows) => {
    const merged = [];
    const seen = new Set();

    for (const row of primary || []) {
        if (row && !seen.has(row.id)) {
            seen.add(row.id);
            merged.push(row);
        }
    }

    for (const row of secondary || []) {
        if (row && !seen.has(row.id)) {
            seen.add(row.id);
            merged.push(row);
        }
    }

    if (Number.isFinite(maxRows) && maxRows > 0 && merged.length > maxRows) {
        return merged.slice(0, maxRows);
    }

    return merged;
};

const expandWithNeighborChunks = async (rows, neighborCount, maxRows) => {
    if (!Array.isArray(rows) || rows.length === 0 || neighborCount <= 0) {
        return rows;
    }

    const indicesBySource = new Map();
    for (const row of rows) {
        const sourceId = row?.sourceId;
        const chunkIndex = Number.parseInt(
            row?.metadata?.chunkIndex,
            10
        );
        if (!sourceId || !Number.isFinite(chunkIndex)) {
            continue;
        }
        const set = indicesBySource.get(sourceId) || new Set();
        for (let offset = 1; offset <= neighborCount; offset += 1) {
            if (chunkIndex - offset >= 0) {
                set.add(chunkIndex - offset);
            }
            set.add(chunkIndex + offset);
        }
        indicesBySource.set(sourceId, set);
    }

    if (indicesBySource.size === 0) {
        return rows;
    }

    const extras = [];
    for (const [sourceId, indexSet] of indicesBySource) {
        const indexes = [...indexSet].filter((value) =>
            Number.isFinite(value)
        );
        if (indexes.length === 0) {
            continue;
        }
        const indexSql = sql.join(
            indexes.map((value) => sql`${value}`),
            sql`, `
        );
        const result = await db.execute(sql`
            select
              c.id,
              c.source_id as "sourceId",
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
            where c.source_id = ${sourceId}
              and (c.metadata->>'chunkIndex')::int in (${indexSql})
        `);
        extras.push(...extractRows(result));
    }

    const seen = new Set(rows.map((row) => row.id));
    const merged = [...rows];
    for (const row of extras) {
        if (row && !seen.has(row.id)) {
            seen.add(row.id);
            merged.push(row);
        }
    }

    if (Number.isFinite(maxRows) && maxRows > 0 && merged.length > maxRows) {
        return merged.slice(0, maxRows);
    }

    return merged;
};

const buildContextRows = async ({
    question,
    retrievalQuestion,
    repoFilter,
    allowGlobalFallback,
    limit,
    skipSemantic = false,
}) => {
    if (skipSemantic) {
        return [];
    }

    let keywords = extractKeywords(question);
    const baseRows = await retrieveChunks(retrievalQuestion, repoFilter, limit);
    if (isEntryPointQuestion(question)) {
        const entryKeywords = [
            "entry",
            "entrypoint",
            "main",
            "index",
            "app",
            "server",
            "bootstrap",
            "start",
        ];
        keywords = [
            ...new Set([...keywords, ...entryKeywords]),
        ];
    }
    const lexicalRows = await retrieveLexicalChunks(
        keywords,
        repoFilter,
        limit
    );
    let merged = mergeRows(baseRows, lexicalRows, chatMaxContextChunks);

    if (isEntryPointQuestion(question)) {
        const entryRows = await retrieveEntryPointChunks(
            repoFilter,
            Math.max(limit, 6)
        );
        merged = mergeRows(merged, entryRows, chatMaxContextChunks);
    }

    if (allowGlobalFallback && merged.length < limit) {
        const globalBase = await retrieveChunks(retrievalQuestion, null, limit);
        const globalLexical = await retrieveLexicalChunks(
            keywords,
            null,
            limit
        );
        merged = mergeRows(
            merged,
            mergeRows(globalBase, globalLexical),
            chatMaxContextChunks
        );
    }

    return expandWithNeighborChunks(
        merged,
        chatNeighborChunks,
        chatMaxContextChunks
    );
};

const formatNumber = (value) => {
    if (value === null || value === undefined) {
        return "0";
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
        return numeric.toLocaleString();
    }
    return String(value);
};

const fetchRepoStats = async (limit) => {
    const cappedLimit =
        Number.isFinite(limit) && limit > 0 ? Math.min(limit, 50) : 20;
    const result = await db.execute(sql`
        select
            s.repo_owner as "repoOwner",
            s.repo_name as "repoName",
            count(distinct s.id) as "fileCount",
            count(c.id) as "chunkCount",
            sum(length(c.content)) as "contentChars"
        from ${sources} s
        join ${chunks} c on c.source_id = s.id
        group by s.repo_owner, s.repo_name
        order by sum(length(c.content)) desc
        limit ${cappedLimit}
    `);
    return extractRows(result);
};

const buildStatsContext = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) {
        return null;
    }

    const lines = rows.map((row) => {
        const repo =
            row.repoOwner && row.repoName
                ? `${row.repoOwner}/${row.repoName}`
                : "unknown";
        return `- ${repo}: ${formatNumber(row.contentChars)} indexed chars, ${formatNumber(
            row.fileCount
        )} files`;
    });

    return {
        label: "index stats",
        header: "type=stats",
        content:
            "Approximate index coverage by repository (larger means more indexed code).\n" +
            lines.join("\n"),
        path: "index-stats",
    };
};

const buildChatContext = (rows, extras = []) => {
    const citations = rows.map((row, index) => ({
        index: index + 1,
        repo:
            row.repo_owner && row.repo_name
                ? `${row.repo_owner}/${row.repo_name}`
                : null,
        path: row.path || null,
        ref: row.ref || null,
        url: row.url || null,
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

    let nextIndex = citations.length;
    for (const extra of extras) {
        nextIndex += 1;
        citations.push({
            index: nextIndex,
            repo: extra.repo || null,
            path: extra.path || extra.label || "context",
            url: extra.url || null,
        });
        const header = `[source:${nextIndex}] ${extra.header || extra.label}`;
        contextBlocks.push(`${header}\n${extra.content}`);
    }

    return { citations, contextBlocks };
};

const fetchSourceSnippet = async (sourceId, options = {}) => {
    if (!sourceId) {
        return null;
    }
    const maxChunks = Number.isFinite(options.maxChunks)
        ? options.maxChunks
        : 4;
    const maxChars = Number.isFinite(options.maxChars)
        ? options.maxChars
        : 4000;
    const maxLines = Number.isFinite(options.maxLines)
        ? options.maxLines
        : 160;

    const result = await db.execute(sql`
        select
            c.content,
            c.metadata
        from ${chunks} c
        where c.source_id = ${sourceId}
        order by (c.metadata->>'chunkIndex')::int asc
        limit ${maxChunks}
    `);
    const rows = extractRows(result);
    if (rows.length === 0) {
        return null;
    }

    const combined = rows.map((row) => row.content).join("\n");
    let snippet = combined;
    let truncated = false;

    const lines = snippet.split("\n");
    if (lines.length > maxLines) {
        snippet = lines.slice(0, maxLines).join("\n");
        truncated = true;
    }
    if (snippet.length > maxChars) {
        snippet = snippet.slice(0, maxChars);
        truncated = true;
    }

    return { snippet, truncated };
};

const buildSnippetContext = (row, snippetResult) => {
    if (!row || !snippetResult?.snippet) {
        return null;
    }
    const language = guessCodeFenceLanguage(row.path);
    const fence = language ? `\`\`\`${language}` : "```";
    const note = snippetResult.truncated
        ? "\n\nNote: snippet truncated for brevity."
        : "";
    return {
        label: "entrypoint snippet",
        header: `type=snippet path=${row.path || "unknown"}`,
        content: `${fence}\n${snippetResult.snippet}\n\`\`\`${note}`,
        repo:
            row.repo_owner && row.repo_name
                ? `${row.repo_owner}/${row.repo_name}`
                : null,
        path: row.path || null,
        url: row.url || null,
    };
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

const enqueueIngestJobs = async (projects) => {
    if (!Array.isArray(projects) || projects.length === 0) {
        return [];
    }
    const now = new Date();
    const jobRows = projects.map((project) => ({
        projectRepo: project.repo,
        projectName: project.name || project.repo,
        status: "queued",
        createdAt: now,
    }));

    const inserted = await db.insert(ingestJobs).values(jobRows).returning({
        id: ingestJobs.id,
        projectRepo: ingestJobs.projectRepo,
        projectName: ingestJobs.projectName,
    });

    const enqueued = [];
    for (const jobRecord of inserted) {
        const job = await ingestQueue.add(
            JOB_TYPES.ingestRepoDocs,
            {
                ingestJobId: jobRecord.id,
                repo: jobRecord.projectRepo,
                name: jobRecord.projectName,
            },
            { jobId: `ingest-${jobRecord.id}` }
        );
        enqueued.push({
            jobId: job.id,
            ingestJobId: jobRecord.id,
            repo: jobRecord.projectRepo,
        });
    }

    return enqueued;
};

const findProjectForReindex = (projects, projectId, repoInput) => {
    if (!Array.isArray(projects) || projects.length === 0) {
        return null;
    }
    const normalizedId =
        typeof projectId === "string" ? projectId.trim() : "";
    const repoFilter = parseRepoFilter(repoInput);

    if (normalizedId) {
        const matchById = projects.find(
            (project) => project && project.id === normalizedId
        );
        if (matchById) {
            return matchById;
        }
    }

    if (repoFilter) {
        const matchByRepo = projects.find((project) => {
            const parsed = parseRepoFromProject(project);
            return parsed && isSameRepo(parsed, repoFilter);
        });
        if (matchByRepo) {
            return matchByRepo;
        }
    }

    return null;
};

app.get("/healthz", async () => ({ ok: true }));

app.get("/projects", async () => {
    const projects = await readProjects();
    return { projects };
});

app.post("/projects", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
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
    let ingestJob = null;
    let ingestError = null;
    try {
        const enqueued = await enqueueIngestJobs([project]);
        ingestJob = enqueued[0] || null;
    } catch (err) {
        ingestError = err.message || "Failed to enqueue ingest job";
    }
    reply.code(201).send({ status: "created", project, ingestJob, ingestError });
});

app.delete("/projects/:id", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
    const projectId =
        typeof request.params?.id === "string"
            ? request.params.id.trim()
            : "";
    const repoCandidate =
        typeof request.query?.repo === "string"
            ? request.query.repo
            : typeof request.body?.repo === "string"
            ? request.body.repo
            : "";
    if (!projectId && !repoCandidate) {
        reply.code(400).send({ error: "project id or repo is required" });
        return;
    }

    const projects = await readProjects();
    const targetRepo = repoCandidate
        ? parseRepoFilter(repoCandidate)
        : null;
    const matchIndex = projects.findIndex((project) => {
        if (projectId && project?.id === projectId) {
            return true;
        }
        if (targetRepo) {
            const projectRepo = parseRepoFromProject(project);
            return projectRepo && isSameRepo(projectRepo, targetRepo);
        }
        return false;
    });

    if (matchIndex === -1) {
        reply.code(404).send({ error: "Project not found" });
        return;
    }

    const [removed] = projects.splice(matchIndex, 1);
    await writeProjects(projects);
    reply.send({ status: "deleted", project: removed });
});

app.post("/chat/sessions", async (request, reply) => {
    const visitorId = normalizeVisitorId(
        request.body?.visitorId || request.headers["x-visitor-id"]
    );
    if (!visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }

    try {
        const sessionId = await createChatSession(visitorId);
        reply.send({ sessionId });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to create session" });
    }
});

app.get("/chat/sessions", async (request, reply) => {
    const visitorId = normalizeVisitorId(
        request.query?.visitorId || request.headers["x-visitor-id"]
    );
    if (!visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }
    await maybePurgeExpiredChatSessions();

    const rawLimit = Number.parseInt(request.query?.limit, 10);
    const limit = Number.isFinite(rawLimit)
        ? Math.min(Math.max(rawLimit, 1), 200)
        : 50;

    try {
        const sessions = await listChatSessions(visitorId, limit);
        reply.send({ sessions });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load sessions" });
    }
});

app.get("/chat/sessions/:id/messages", async (request, reply) => {
    const sessionId = normalizeSessionId(request.params?.id);
    if (!sessionId) {
        reply.code(400).send({ error: "sessionId is required" });
        return;
    }

    const visitorId = normalizeVisitorId(
        request.query?.visitorId || request.headers["x-visitor-id"]
    );
    if (!visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }
    await maybePurgeExpiredChatSessions();

    const session = await fetchChatSession(sessionId);
    if (!session || (session.visitorId && session.visitorId !== visitorId)) {
        reply.code(404).send({ error: "Session not found" });
        return;
    }

    const rawLimit = Number.parseInt(request.query?.limit, 10);
    const limit = Number.isFinite(rawLimit)
        ? Math.min(Math.max(rawLimit, 1), 200)
        : 200;

    try {
        const messages = await fetchChatHistory(sessionId, limit);
        reply.send({ sessionId, messages });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load messages" });
    }
});

app.delete("/chat/sessions/:id", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
    const sessionId = normalizeSessionId(request.params?.id);
    if (!sessionId) {
        reply.code(400).send({ error: "sessionId is required" });
        return;
    }

    const session = await fetchChatSession(sessionId);
    if (!session) {
        reply.code(404).send({ error: "Session not found" });
        return;
    }

    await db
        .delete(chatMessages)
        .where(eq(chatMessages.sessionId, sessionId));
    await db.delete(chatSessions).where(eq(chatSessions.id, sessionId));

    reply.send({ status: "deleted", sessionId });
});

app.post("/chat", async (_request, reply) => {
    const body = _request.body || {};
    const acceptHeader = _request.headers.accept || "";
    const wantsStream =
        body.stream === true || acceptHeader.includes("text/event-stream");
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
    await maybePurgeExpiredChatSessions();

    const statsQuestion = isStatsQuestion(question);
    const visitorId = normalizeVisitorId(
        body.visitorId || _request.headers["x-visitor-id"]
    );
    const sessionIdInput =
        body.sessionId || body.session_id || body.chatSessionId;
    let sessionId = normalizeSessionId(sessionIdInput);

    if (sessionId) {
        const session = await fetchChatSession(sessionId);
        if (
            !session ||
            (visitorId && session.visitorId && session.visitorId !== visitorId)
        ) {
            reply.code(404).send({ error: "Session not found" });
            return;
        }
    }

    if (!sessionId) {
        try {
            sessionId = await createChatSession(visitorId);
        } catch (err) {
            app.log.error(err);
            sessionId = null;
        }
    }

    let history = [];
    if (sessionId) {
        history = await fetchChatHistory(sessionId, chatHistoryLimit);
    }
    if (history.length === 0) {
        history = normalizeChatHistory(
            body.history || body.messages,
            chatHistoryLimit
        );
    }

    const historyRepo =
        history.length > 0 ? inferRepoFromHistory(history) : null;
    const questionRepoMatch = await resolveRepoFromQuestion(question);
    const questionRepo = questionRepoMatch?.repo || null;
    const questionRepoExplicit = Boolean(questionRepoMatch?.explicit);
    const repoFilterInput =
        body.repo || body.repoUrl || body.project || body.projectRepo;
    const repoInput = parseRepoFilter(repoFilterInput);
    let repoFilter = repoInput;
    const hasContextRepo = Boolean(historyRepo || repoInput);
    const shouldUseQuestionRepo =
        questionRepo && (!hasContextRepo || questionRepoExplicit);
    if (
        shouldUseQuestionRepo &&
        (!repoFilter || !isSameRepo(repoFilter, questionRepo))
    ) {
        repoFilter = questionRepo;
    }
    if (!repoFilter && historyRepo) {
        repoFilter = historyRepo;
    }
    if (historyRepo && repoFilter && !isSameRepo(historyRepo, repoFilter)) {
        history = [];
    }
    if (statsQuestion) {
        repoFilter = null;
        history = [];
    }

    const historyText = historyToText(history);
    const retrievalQuestion = buildRetrievalQuestion(question, history);
    const limit = Number.isFinite(Number(body.topK))
        ? Math.min(Math.max(Number(body.topK), 1), 20)
        : Math.min(Math.max(chatTopK, 1), 20);
    const repoIsExplicit = Boolean(
        repoFilter && (questionRepoExplicit || repoInput || historyRepo)
    );
    const allowGlobalFallback = statsQuestion || !repoFilter || !repoIsExplicit;
    const conversationBlock = historyText
        ? `Conversation:\n${historyText}\n\n`
        : "";

    if (sessionId) {
        try {
            await storeChatMessage({
                sessionId,
                role: "user",
                content: question,
            });
        } catch (err) {
            app.log.error(err);
        }
    }

    try {
        if (wantsStream) {
            reply.hijack();
            const allowedOrigin = resolveCorsOrigin(_request);
            if (allowedOrigin) {
                reply.raw.setHeader(
                    "Access-Control-Allow-Origin",
                    allowedOrigin
                );
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
                const rows = await buildContextRows({
                    question,
                    retrievalQuestion,
                    repoFilter,
                    allowGlobalFallback,
                    limit,
                    skipSemantic: statsQuestion,
                });
                const extras = [];
                if (statsQuestion) {
                    const statsRows = await fetchRepoStats(10);
                    const statsContext = buildStatsContext(statsRows);
                    if (statsContext) {
                        extras.push(statsContext);
                    }
                }
                if (isEntryPointQuestion(question)) {
                    let entryRow = selectEntryPointRow(rows);
                    if (!entryRow) {
                        const entryRows = await retrieveEntryPointChunks(
                            repoFilter,
                            6
                        );
                        entryRow = entryRows[0] || null;
                    }
                    if (entryRow) {
                        const snippetResult = await fetchSourceSnippet(
                            entryRow.sourceId,
                            {
                                maxChunks: chatSnippetMaxChunks,
                                maxChars: chatSnippetMaxChars,
                                maxLines: chatSnippetMaxLines,
                            }
                        );
                        const snippetContext = buildSnippetContext(
                            entryRow,
                            snippetResult
                        );
                        if (snippetContext) {
                            extras.push(snippetContext);
                        }
                    }
                }

                if (rows.length === 0 && extras.length === 0) {
                    sendEvent("meta", {
                        sessionId,
                        citations: [],
                        context: { count: 0 },
                    });
                    sendEvent("delta", {
                        delta: "I don't know based on the indexed sources.",
                    });
                    sendEvent("done", {});
                    if (sessionId) {
                        await storeChatMessage({
                            sessionId,
                            role: "assistant",
                            content: "I don't know based on the indexed sources.",
                            citations: [],
                        });
                    }
                    reply.raw.end();
                    return;
                }

                const { citations, contextBlocks } = buildChatContext(
                    rows,
                    extras
                );

                const systemPrompt = [
                    "You answer questions about GitHub repositories using ONLY the provided context blocks.",
                    "Use the conversation history to interpret follow-up questions, but answers must come from the context blocks.",
                    "If the answer is not in the context, say you don't know.",
                    "When asked about code or entry points, include the relevant snippet in a fenced code block.",
                    "Cite sources using [source:n] where n matches the context block.",
                ].join(" ");

                const userPrompt = `${conversationBlock}Question: ${question}\n\nContext:\n${contextBlocks.join(
                    "\n\n"
                )}`;

                sendEvent("meta", {
                    sessionId,
                    citations,
                    context: { count: contextBlocks.length },
                });

                let assistantContent = "";
                const stream = await openai.chat.completions.create(
                    {
                        model: chatModel,
                        temperature: Number.isFinite(chatTemperature)
                            ? chatTemperature
                            : 0.2,
                        max_tokens: Number.isFinite(chatMaxTokens)
                            ? chatMaxTokens
                            : 800,
                        stream: true,
                        messages: [
                            { role: "system", content: systemPrompt },
                            { role: "user", content: userPrompt },
                        ],
                    },
                    {
                        signal: abortController.signal,
                    }
                );

                for await (const chunk of stream) {
                    const delta = chunk.choices?.[0]?.delta?.content;
                    if (delta) {
                        assistantContent += delta;
                        sendEvent("delta", { delta });
                    }
                }

                sendEvent("done", {});
                if (sessionId && assistantContent.trim()) {
                    await storeChatMessage({
                        sessionId,
                        role: "assistant",
                        content: assistantContent.trim(),
                        citations,
                    });
                }
            } catch (err) {
                sendEvent("error", { error: err.message || "Chat failed" });
            } finally {
                reply.raw.end();
                reply.raw.off("close", onClose);
            }

            return;
        }

        const rows = await buildContextRows({
            question,
            retrievalQuestion,
            repoFilter,
            allowGlobalFallback,
            limit,
            skipSemantic: statsQuestion,
        });
        const extras = [];
        if (statsQuestion) {
            const statsRows = await fetchRepoStats(10);
            const statsContext = buildStatsContext(statsRows);
            if (statsContext) {
                extras.push(statsContext);
            }
        }
        if (isEntryPointQuestion(question)) {
            let entryRow = selectEntryPointRow(rows);
            if (!entryRow) {
                const entryRows = await retrieveEntryPointChunks(
                    repoFilter,
                    6
                );
                entryRow = entryRows[0] || null;
            }
            if (entryRow) {
                const snippetResult = await fetchSourceSnippet(
                    entryRow.sourceId,
                    {
                        maxChunks: chatSnippetMaxChunks,
                        maxChars: chatSnippetMaxChars,
                        maxLines: chatSnippetMaxLines,
                    }
                );
                const snippetContext = buildSnippetContext(
                    entryRow,
                    snippetResult
                );
                if (snippetContext) {
                    extras.push(snippetContext);
                }
            }
        }

        if (rows.length === 0 && extras.length === 0) {
            reply.send({
                answer: "I don't know based on the indexed sources.",
                citations: [],
                context: { count: 0 },
                sessionId,
            });
            if (sessionId) {
                await storeChatMessage({
                    sessionId,
                    role: "assistant",
                    content: "I don't know based on the indexed sources.",
                    citations: [],
                });
            }
            return;
        }

        const { citations, contextBlocks } = buildChatContext(rows, extras);

        const systemPrompt = [
            "You answer questions about GitHub repositories using ONLY the provided context blocks.",
            "Use the conversation history to interpret follow-up questions, but answers must come from the context blocks.",
            "If the answer is not in the context, say you don't know.",
            "When asked about code or entry points, include the relevant snippet in a fenced code block.",
            "Cite sources using [source:n] where n matches the context block.",
        ].join(" ");

        const userPrompt = `${conversationBlock}Question: ${question}\n\nContext:\n${contextBlocks.join(
            "\n\n"
        )}`;

        const completion = await openai.chat.completions.create({
            model: chatModel,
            temperature: Number.isFinite(chatTemperature)
                ? chatTemperature
                : 0.2,
            max_tokens: Number.isFinite(chatMaxTokens) ? chatMaxTokens : 800,
            messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: userPrompt },
            ],
        });

        const answer = completion.choices?.[0]?.message?.content?.trim() || "";

        reply.send({
            answer,
            citations,
            context: { count: contextBlocks.length },
            sessionId,
        });
        if (sessionId && answer) {
            await storeChatMessage({
                sessionId,
                role: "assistant",
                content: answer,
                citations,
            });
        }
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
    let enqueued = [];
    try {
        enqueued = await enqueueIngestJobs(projects);
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to enqueue jobs" });
        return;
    }

    reply.send({ status: "queued", count: enqueued.length, jobs: enqueued });
});

app.post("/admin/projects/:id/reindex", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
    const projectId =
        typeof request.params?.id === "string"
            ? request.params.id.trim()
            : "";
    const repoInput =
        typeof request.body?.repo === "string"
            ? request.body.repo
            : typeof request.query?.repo === "string"
            ? request.query.repo
            : "";
    const projects = (await readProjects()).filter(
        (project) => project && project.repo
    );
    const project = findProjectForReindex(projects, projectId, repoInput);
    if (!project) {
        reply.code(404).send({ error: "Project not found" });
        return;
    }

    try {
        const enqueued = await enqueueIngestJobs([project]);
        reply.send({ status: "queued", job: enqueued[0] });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to enqueue job" });
    }
});

app.post("/admin/jobs/:id/retry", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
    const ingestJobId = normalizeSessionId(request.params?.id);
    if (!ingestJobId) {
        reply.code(400).send({ error: "job id is required" });
        return;
    }

    const rows = await db
        .select({
            id: ingestJobs.id,
            projectRepo: ingestJobs.projectRepo,
            projectName: ingestJobs.projectName,
        })
        .from(ingestJobs)
        .where(eq(ingestJobs.id, ingestJobId))
        .limit(1);
    const jobRecord = rows[0];
    if (!jobRecord?.projectRepo) {
        reply.code(404).send({ error: "Job not found" });
        return;
    }

    try {
        const enqueued = await enqueueIngestJobs([
            {
                repo: jobRecord.projectRepo,
                name: jobRecord.projectName || jobRecord.projectRepo,
            },
        ]);
        reply.send({ status: "queued", job: enqueued[0] });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to retry job" });
    }
});

app.post("/admin/jobs/:id/cancel", async (request, reply) => {
    if (!requireAdmin(request, reply)) {
        return;
    }
    const ingestJobId = normalizeSessionId(request.params?.id);
    if (!ingestJobId) {
        reply.code(400).send({ error: "job id is required" });
        return;
    }

    const rows = await db
        .select({
            id: ingestJobs.id,
            status: ingestJobs.status,
        })
        .from(ingestJobs)
        .where(eq(ingestJobs.id, ingestJobId))
        .limit(1);
    const jobRecord = rows[0];
    if (!jobRecord) {
        reply.code(404).send({ error: "Job not found" });
        return;
    }
    if (
        ["completed", "failed", "canceled"].includes(
            String(jobRecord.status || "")
        )
    ) {
        reply.code(409).send({ error: "Job already finished" });
        return;
    }

    let removed = false;
    try {
        const queueJob = await ingestQueue.getJob(`ingest-${ingestJobId}`);
        if (queueJob) {
            await queueJob.remove();
            removed = true;
        }
    } catch (err) {
        app.log.warn(err);
    }

    const nextStatus = removed ? "canceled" : "cancel_requested";
    const updateValues = {
        status: nextStatus,
        lastMessage: removed
            ? "Canceled before start"
            : "Cancel requested",
    };
    if (removed) {
        updateValues.finishedAt = new Date();
    }
    await db
        .update(ingestJobs)
        .set(updateValues)
        .where(eq(ingestJobs.id, ingestJobId));

    reply.send({ status: nextStatus, jobId: ingestJobId });
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
            updatedAt: ingestJobs.updatedAt,
        })
        .from(ingestJobs)
        .orderBy(desc(ingestJobs.id))
        .limit(limit);

    reply.send({ jobs });
});

if (chatSessionTtlMs && chatSessionCleanupIntervalMs) {
    setInterval(async () => {
        lastChatCleanupAt = Date.now();
        try {
            const result = await purgeExpiredChatSessions();
            if (result?.expired) {
                app.log.info(
                    { expired: result.expired },
                    "Purged expired chat sessions"
                );
            }
        } catch (err) {
            app.log.error(err);
        }
    }, chatSessionCleanupIntervalMs);
}

const runIngestSchedule = async (label, fn) => {
    if (ingestScheduleInFlight) {
        return;
    }
    ingestScheduleInFlight = true;
    try {
        const result = await fn();
        if (result?.enqueued) {
            app.log.info(
                { label, ...result },
                "Scheduled ingest queue updated"
            );
        }
    } catch (err) {
        app.log.error(err);
    } finally {
        ingestScheduleInFlight = false;
    }
};

if (ingestReindexIntervalMs) {
    setInterval(() => {
        runIngestSchedule("reindex", enqueueReindexForAllProjects);
    }, ingestReindexIntervalMs);
    setTimeout(() => {
        runIngestSchedule("reindex", enqueueReindexForAllProjects);
    }, 5000);
}

if (ingestInitialCheckMs) {
    setInterval(() => {
        runIngestSchedule("initial-index", async () => {
            const initial = await enqueueInitialIndexForMissingProjects();
            const requeued = await requeueStaleIngestJobs();
            return {
                enqueued: (initial.enqueued || 0) + (requeued.requeued || 0),
                initial,
                requeued,
            };
        });
    }, ingestInitialCheckMs);
    setTimeout(() => {
        runIngestSchedule("initial-index", async () => {
            const initial = await enqueueInitialIndexForMissingProjects();
            const requeued = await requeueStaleIngestJobs();
            return {
                enqueued: (initial.enqueued || 0) + (requeued.requeued || 0),
                initial,
                requeued,
            };
        });
    }, 8000);
}

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
