import crypto from "node:crypto";
import fsSync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Queue } from "bullmq";
import { and, desc, eq, gt, inArray, sql } from "drizzle-orm";
import cors from "@fastify/cors";
import Fastify from "fastify";
import OpenAI from "openai";
import Stripe from "stripe";
import {
    JOB_TYPES,
    QUEUE_NAMES,
    authSessions,
    chatMessages,
    chatSessions,
    chunks,
    getRedisConnectionOptions,
    ingestJobs,
    projects,
    sources,
    tenants,
    telemetryEvents,
    usageEvents,
    users,
} from "@app/shared";
import { db } from "./db/index.js";

const app = Fastify({ logger: true });

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..", "..");

const corsOrigin = process.env.CORS_ORIGIN
    ? process.env.CORS_ORIGIN.split(",").map((origin) => origin.trim())
    : true;

app.register(cors, { origin: corsOrigin, credentials: true });

app.addContentTypeParser(
    "application/json",
    { parseAs: "string" },
    (request, body, done) => {
        request.rawBody = body;
        if (!body) {
            done(null, {});
            return;
        }
        try {
            done(null, JSON.parse(body));
        } catch (err) {
            done(err);
        }
    }
);

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

const normalizeBaseUrl = (value) => {
    if (!value) {
        return "";
    }
    const trimmed = String(value).trim();
    if (!trimmed) {
        return "";
    }
    return trimmed.replace(/\/+$/, "");
};

const SESSION_COOKIE_NAME = "session_token";
const OAUTH_STATE_COOKIE = "oauth_state";
const OAUTH_RETURN_COOKIE = "oauth_return_to";

const webBaseUrl = normalizeBaseUrl(
    process.env.WEB_BASE_URL || process.env.NEXT_PUBLIC_WEB_BASE_URL
);
const apiPublicUrl = normalizeBaseUrl(
    process.env.API_PUBLIC_URL ||
        process.env.NEXT_PUBLIC_API_BASE_URL ||
        process.env.API_BASE_URL
);
const oauthClientId = process.env.GITHUB_OAUTH_CLIENT_ID;
const oauthClientSecret = process.env.GITHUB_OAUTH_CLIENT_SECRET;
const oauthRedirectUrl = apiPublicUrl
    ? `${apiPublicUrl}/auth/github/callback`
    : "";
const authSessionTtlDays = Number.parseInt(
    process.env.AUTH_SESSION_TTL_DAYS || "30",
    10
);
const authSessionTtlMs =
    Number.isFinite(authSessionTtlDays) && authSessionTtlDays > 0
        ? authSessionTtlDays * 24 * 60 * 60 * 1000
        : 0;
const cookieSecure =
    process.env.COOKIE_SECURE === "true" ||
    process.env.NODE_ENV === "production";
const cookieDomain = process.env.COOKIE_DOMAIN || "";

const stripeSecretKey = process.env.STRIPE_SECRET_KEY;
const stripeWebhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
const stripePriceStarter = process.env.STRIPE_PRICE_STARTER;
const stripePricePro = process.env.STRIPE_PRICE_PRO;
const stripePriceUnlimited = process.env.STRIPE_PRICE_UNLIMITED;
const stripePriceTokens = process.env.STRIPE_PRICE_TOKENS;
const stripeTokenMeterEventName = (
    process.env.STRIPE_TOKEN_METER_EVENT_NAME || ""
).trim();
const stripeTokenMeterId = (process.env.STRIPE_TOKEN_METER_ID || "").trim();
const stripeTokenMeterCustomerKey = (
    process.env.STRIPE_TOKEN_METER_CUSTOMER_KEY || "stripe_customer_id"
).trim();
const stripeTokenMeterValueKey = (
    process.env.STRIPE_TOKEN_METER_VALUE_KEY || "value"
).trim();
const tokenMeterConfigured = Boolean(
    stripeTokenMeterEventName || stripeTokenMeterId
);
const unlimitedTokenLimitRaw = Number.parseInt(
    process.env.STRIPE_UNLIMITED_TOKEN_LIMIT || "1000000000",
    10
);
const unlimitedTokenLimit =
    Number.isFinite(unlimitedTokenLimitRaw) && unlimitedTokenLimitRaw > 0
        ? unlimitedTokenLimitRaw
        : 1000000000;
const stripeCheckoutSuccessUrl =
    process.env.STRIPE_CHECKOUT_SUCCESS_URL ||
    (webBaseUrl ? `${webBaseUrl}/account?checkout=success` : "");
const stripeCheckoutCancelUrl =
    process.env.STRIPE_CHECKOUT_CANCEL_URL ||
    (webBaseUrl ? `${webBaseUrl}/account?checkout=cancel` : "");
const stripePortalReturnUrl =
    process.env.STRIPE_PORTAL_RETURN_URL ||
    (webBaseUrl ? `${webBaseUrl}/account` : "");
const stripe =
    stripeSecretKey && stripeSecretKey.trim()
        ? new Stripe(stripeSecretKey, { apiVersion: "2024-06-20" })
        : null;
const billingEnabled = Boolean(stripe);

const ingestQueue = new Queue(QUEUE_NAMES.ingest, {
    connection: getRedisConnectionOptions(),
});

const openaiApiKey = process.env.OPENAI_API_KEY;
const embeddingModel =
    process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";
const chatModel = process.env.OPENAI_CHAT_MODEL || "gpt-4o";
const normalizedChatModel = String(chatModel).toLowerCase();
const chatModelIsGpt5 = normalizedChatModel.startsWith("gpt-5");
const chatModelSupportsTemperature = !chatModelIsGpt5;
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
let stripeUsageSyncInFlight = false;

const openai = openaiApiKey ? new OpenAI({ apiKey: openaiApiKey }) : null;

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
        expiresAt,
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
        expiresAt: Date.now() + installationIdCacheTtlMs,
    });
};

const parseCookies = (headerValue) => {
    if (!headerValue || typeof headerValue !== "string") {
        return {};
    }
    return headerValue.split(";").reduce((acc, part) => {
        const [name, ...rest] = part.trim().split("=");
        if (!name) {
            return acc;
        }
        const value = rest.join("=");
        acc[name] = value ? decodeURIComponent(value) : "";
        return acc;
    }, {});
};

const getCookie = (request, name) => {
    const cookies = parseCookies(request.headers.cookie);
    return cookies[name];
};

const serializeCookie = (name, value, options = {}) => {
    const encoded = encodeURIComponent(value ?? "");
    const parts = [`${name}=${encoded}`];
    if (options.maxAge !== undefined) {
        parts.push(`Max-Age=${Math.floor(options.maxAge)}`);
    }
    if (options.expires instanceof Date) {
        parts.push(`Expires=${options.expires.toUTCString()}`);
    }
    parts.push(`Path=${options.path || "/"}`);
    if (options.domain) {
        parts.push(`Domain=${options.domain}`);
    }
    if (options.httpOnly !== false) {
        parts.push("HttpOnly");
    }
    if (options.sameSite) {
        parts.push(`SameSite=${options.sameSite}`);
    }
    if (options.secure) {
        parts.push("Secure");
    }
    return parts.join("; ");
};

const appendSetCookie = (reply, cookieValue) => {
    if (!cookieValue) {
        return;
    }
    const existing = reply.getHeader("Set-Cookie");
    if (!existing) {
        reply.header("Set-Cookie", cookieValue);
        return;
    }
    if (Array.isArray(existing)) {
        reply.header("Set-Cookie", [...existing, cookieValue]);
        return;
    }
    reply.header("Set-Cookie", [existing, cookieValue]);
};

const setCookie = (reply, name, value, options = {}) => {
    const cookieValue = serializeCookie(name, value, options);
    appendSetCookie(reply, cookieValue);
};

const clearCookie = (reply, name) => {
    appendSetCookie(
        reply,
        serializeCookie(name, "", {
            path: "/",
            expires: new Date(0),
            maxAge: 0,
            httpOnly: true,
            sameSite: "Lax",
            secure: cookieSecure,
            domain: cookieDomain || undefined,
        })
    );
};

const hashToken = (value) =>
    crypto.createHash("sha256").update(value).digest("hex");

const createAuthSession = async ({ userId, tenantId }) => {
    if (!authSessionTtlMs) {
        throw new Error("AUTH_SESSION_TTL_DAYS must be set");
    }
    const rawToken = crypto.randomBytes(32).toString("hex");
    const tokenHash = hashToken(rawToken);
    const expiresAt = new Date(Date.now() + authSessionTtlMs);
    await db.insert(authSessions).values({
        userId,
        tenantId,
        tokenHash,
        expiresAt,
    });
    return { token: rawToken, expiresAt };
};

const getAuthSession = async (request) => {
    const rawToken = getCookie(request, SESSION_COOKIE_NAME);
    if (!rawToken) {
        return null;
    }
    const tokenHash = hashToken(rawToken);
    const rows = await db
        .select({
            sessionId: authSessions.id,
            tenantId: authSessions.tenantId,
            userId: users.id,
            email: users.email,
            name: users.name,
            githubUsername: users.githubUsername,
            avatarUrl: users.avatarUrl,
        })
        .from(authSessions)
        .innerJoin(users, eq(authSessions.userId, users.id))
        .where(
            and(
                eq(authSessions.tokenHash, tokenHash),
                gt(authSessions.expiresAt, new Date())
            )
        )
        .limit(1);
    if (!rows.length) {
        return null;
    }
    const row = rows[0];
    return {
        sessionId: row.sessionId,
        tenantId: row.tenantId,
        user: {
            id: row.userId,
            email: row.email,
            name: row.name,
            githubUsername: row.githubUsername,
            avatarUrl: row.avatarUrl,
        },
    };
};

const slugify = (value) =>
    value
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, "-")
        .replace(/-+/g, "-")
        .replace(/^-|-$/g, "");

const normalizeHandle = (value) => {
    if (!value || typeof value !== "string") {
        return "";
    }
    return value.trim().replace(/^@/, "").toLowerCase();
};

const handlePattern = /^[a-z0-9][a-z0-9-_]{1,28}[a-z0-9]$/;
const reservedHandles = new Set([
    "account",
    "admin",
    "api",
    "assets",
    "auth",
    "favicon",
    "favicon.svg",
    "login",
    "logout",
    "signup",
    "signin",
    "settings",
    "telemetry",
]);

const normalizeHandleInput = (value) => {
    if (!value || typeof value !== "string") {
        return "";
    }
    return normalizeHandle(value);
};

const validateHandleInput = (value) => {
    const normalized = normalizeHandleInput(value);
    if (!normalized) {
        return { handle: "" };
    }
    if (reservedHandles.has(normalized)) {
        return { error: "Handle is reserved." };
    }
    if (!handlePattern.test(normalized)) {
        return {
            error:
                "Handle must be 3-30 characters and use letters, numbers, - or _.",
        };
    }
    return { handle: normalized };
};

const MAX_BIO_LENGTH = 500;

const normalizeBooleanInput = (value, fallback = undefined) => {
    if (typeof value === "boolean") {
        return value;
    }
    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (normalized === "true") {
            return true;
        }
        if (normalized === "false") {
            return false;
        }
    }
    return fallback;
};

const normalizeBioInput = (value) => {
    if (typeof value !== "string") {
        return "";
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return "";
    }
    return trimmed.length > MAX_BIO_LENGTH
        ? trimmed.slice(0, MAX_BIO_LENGTH)
        : trimmed;
};

const normalizeCategoryInput = (value) => {
    if (value === null || value === undefined || value === "") {
        return "";
    }
    if (typeof value !== "string") {
        return null;
    }
    return value.trim();
};

const PLAN_DEFINITIONS = {
    starter: {
        label: "Starter",
        priceId: stripePriceStarter,
        repoLimit: 10,
        tokenLimit: null,
        tokenUsage: true,
    },
    pro: {
        label: "Pro",
        priceId: stripePricePro,
        repoLimit: 50,
        tokenLimit: null,
        tokenUsage: true,
    },
    unlimited: {
        label: "Unlimited",
        priceId: stripePriceUnlimited,
        repoLimit: null,
        tokenLimit: null,
        tokenUsage: true,
        includedTokens: unlimitedTokenLimit,
    },
};

const ACTIVE_SUBSCRIPTION_STATUSES = new Set([
    "active",
    "trialing",
    "past_due",
]);

const normalizePlanInput = (value) => {
    if (!value || typeof value !== "string") {
        return "";
    }
    const normalized = value.trim().toLowerCase();
    return PLAN_DEFINITIONS[normalized] ? normalized : "";
};

const resolvePlanFromPriceId = (priceId) => {
    if (!priceId) {
        return "";
    }
    const entries = Object.entries(PLAN_DEFINITIONS);
    for (const [planKey, plan] of entries) {
        if (plan.priceId && plan.priceId === priceId) {
            return planKey;
        }
    }
    return "";
};

const resolvePlanFromSubscriptionItems = (items) => {
    const list = Array.isArray(items) ? items : [];
    for (const item of list) {
        const priceId = item?.price?.id;
        const plan = resolvePlanFromPriceId(priceId);
        if (plan) {
            return plan;
        }
    }
    return "";
};

const resolveTokenItemId = (items) => {
    if (!stripePriceTokens) {
        return "";
    }
    const list = Array.isArray(items) ? items : [];
    for (const item of list) {
        if (item?.price?.id === stripePriceTokens) {
            return item.id || "";
        }
    }
    return "";
};

const isBillingActive = (status) => {
    if (!status) {
        return false;
    }
    return ACTIVE_SUBSCRIPTION_STATUSES.has(String(status));
};

const getPlanLimits = (planKey) => {
    const plan = PLAN_DEFINITIONS[planKey];
    if (!plan) {
        return {
            repoLimit: null,
            tokenLimit: null,
            tokenUsage: false,
            includedTokens: null,
        };
    }
    return {
        repoLimit: plan.repoLimit ?? null,
        tokenLimit: plan.tokenLimit ?? null,
        tokenUsage: Boolean(plan.tokenUsage),
        includedTokens: plan.includedTokens ?? null,
    };
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

const formatProjectRow = (row) => ({
    id: row.id,
    name: row.name,
    repo: row.repoUrl,
    description: row.description || "",
    tags: Array.isArray(row.tags) ? row.tags : [],
    featured: Boolean(row.featured),
    category: row.category || null,
});

const tenantProfileSelect = {
    tenantId: tenants.id,
    handle: tenants.handle,
    bio: tenants.bio,
    isPublic: tenants.isPublic,
    tenantName: tenants.name,
    repoLimit: tenants.repoLimit,
    tokenLimit: tenants.tokenLimit,
    userName: users.name,
    githubUsername: users.githubUsername,
    avatarUrl: users.avatarUrl,
};

const tenantBillingSelect = {
    tenantId: tenants.id,
    plan: tenants.plan,
    subscriptionStatus: tenants.subscriptionStatus,
    stripeCustomerId: tenants.stripeCustomerId,
    stripeSubscriptionId: tenants.stripeSubscriptionId,
    stripeTokenItemId: tenants.stripeTokenItemId,
    repoLimit: tenants.repoLimit,
    tokenLimit: tenants.tokenLimit,
    currentPeriodEnd: tenants.currentPeriodEnd,
    billingEmail: tenants.billingEmail,
};

const buildOwnerPayload = (profile) => {
    if (!profile) {
        return null;
    }
    const handle = profile.handle || profile.githubUsername || "";
    const name =
        profile.userName ||
        profile.tenantName ||
        profile.githubUsername ||
        handle ||
        "";
    return {
        handle,
        name,
        avatarUrl: profile.avatarUrl || "",
        bio: profile.bio || "",
        isPublic:
            typeof profile.isPublic === "boolean" ? profile.isPublic : true,
    };
};

const fetchTenantProfile = async (tenantId) => {
    if (!tenantId) {
        return null;
    }
    const rows = await db
        .select(tenantProfileSelect)
        .from(tenants)
        .leftJoin(users, eq(users.tenantId, tenants.id))
        .where(eq(tenants.id, tenantId))
        .limit(1);
    return rows[0] || null;
};

const fetchTenantBilling = async (tenantId) => {
    if (!tenantId) {
        return null;
    }
    const rows = await db
        .select(tenantBillingSelect)
        .from(tenants)
        .where(eq(tenants.id, tenantId))
        .limit(1);
    return rows[0] || null;
};

const fetchTenantByStripeCustomerId = async (customerId) => {
    if (!customerId) {
        return null;
    }
    const rows = await db
        .select(tenantBillingSelect)
        .from(tenants)
        .where(eq(tenants.stripeCustomerId, customerId))
        .limit(1);
    return rows[0] || null;
};

const fetchTenantByStripeSubscriptionId = async (subscriptionId) => {
    if (!subscriptionId) {
        return null;
    }
    const rows = await db
        .select(tenantBillingSelect)
        .from(tenants)
        .where(eq(tenants.stripeSubscriptionId, subscriptionId))
        .limit(1);
    return rows[0] || null;
};

const updateTenantBilling = async (tenantId, updates) => {
    if (!tenantId || !updates || Object.keys(updates).length === 0) {
        return;
    }
    await db.update(tenants).set(updates).where(eq(tenants.id, tenantId));
};

const fetchTenantByHandle = async (handle) => {
    const normalized = normalizeHandle(handle);
    if (!normalized) {
        return null;
    }
    const rows = await db
        .select(tenantProfileSelect)
        .from(tenants)
        .leftJoin(users, eq(users.tenantId, tenants.id))
        .where(sql`lower(${tenants.handle}) = ${normalized}`)
        .limit(1);
    if (rows.length > 0) {
        return rows[0];
    }

    const fallback = await db
        .select(tenantProfileSelect)
        .from(users)
        .innerJoin(tenants, eq(users.tenantId, tenants.id))
        .where(
            and(
                sql`lower(${users.githubUsername}) = ${normalized}`,
                sql`${tenants.handle} is null`
            )
        )
        .limit(1);
    return fallback[0] || null;
};

const resolveTenantByHandle = async (handle) => {
    const profile = await fetchTenantByHandle(handle);
    if (!profile) {
        return null;
    }
    return {
        tenantId: profile.tenantId,
        ...buildOwnerPayload(profile),
    };
};

const isHandleAvailable = async (handle, tenantId) => {
    const normalized = normalizeHandleInput(handle);
    if (!normalized) {
        return true;
    }
    const tenantRows = await db
        .select({ id: tenants.id })
        .from(tenants)
        .where(sql`lower(${tenants.handle}) = ${normalized}`)
        .limit(1);
    if (
        tenantRows.length > 0 &&
        (!tenantId || tenantRows[0].id !== tenantId)
    ) {
        return false;
    }

    const userRows = await db
        .select({ tenantId: users.tenantId })
        .from(users)
        .where(sql`lower(${users.githubUsername}) = ${normalized}`)
        .limit(1);
    if (userRows.length > 0 && userRows[0].tenantId !== tenantId) {
        return false;
    }
    return true;
};

const fetchTenantLimits = async (tenantId) => {
    if (!tenantId) {
        return { repoLimit: null, tokenLimit: null };
    }
    const rows = await db
        .select({ repoLimit: tenants.repoLimit, tokenLimit: tenants.tokenLimit })
        .from(tenants)
        .where(eq(tenants.id, tenantId))
        .limit(1);
    return rows[0] || { repoLimit: null, tokenLimit: null };
};

const requireActiveSubscription = async (tenantId, reply, options = {}) => {
    if (!billingEnabled) {
        return true;
    }
    const billing = await fetchTenantBilling(tenantId);
    if (!billing || !isBillingActive(billing.subscriptionStatus)) {
        const message =
            typeof options.message === "string" && options.message.trim()
                ? options.message.trim()
                : "Active subscription required.";
        reply.code(402).send({ error: message });
        return false;
    }
    return true;
};

const fetchRepoCount = async (tenantId) => {
    if (!tenantId) {
        return 0;
    }
    const repoRows = extractRows(
        await db.execute(sql`
            select count(*)::int as "count"
            from ${projects}
            where tenant_id = ${tenantId}
        `)
    );
    const count = Number.parseInt(repoRows[0]?.count, 10);
    return Number.isFinite(count) ? count : 0;
};

const getUsagePeriod = (value = new Date()) => {
    const year = value.getUTCFullYear();
    const month = value.getUTCMonth();
    const start = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
    const end = new Date(Date.UTC(year, month + 1, 1, 0, 0, 0, 0));
    return { start, end };
};

const fetchTokenUsageForPeriod = async (tenantId, period) => {
    if (!tenantId || !period?.start || !period?.end) {
        return 0;
    }
    const rows = extractRows(
        await db.execute(sql`
            select coalesce(sum(tokens), 0)::int as "count"
            from ${usageEvents}
            where tenant_id = ${tenantId}
              and created_at >= ${period.start}
              and created_at < ${period.end}
        `)
    );
    const count = Number.parseInt(rows[0]?.count, 10);
    return Number.isFinite(count) ? count : 0;
};

const recordUsageEvent = async ({ tenantId, sessionId, tokens, eventType }) => {
    if (!tenantId || !eventType) {
        return null;
    }
    const safeTokens =
        typeof tokens === "number" && Number.isFinite(tokens) ? tokens : null;
    if (safeTokens === null) {
        return null;
    }
    try {
        const [row] = await db
            .insert(usageEvents)
            .values({
                tenantId,
                sessionId: sessionId || null,
                eventType,
                tokens: safeTokens,
            })
            .returning({ id: usageEvents.id });
        return row?.id || null;
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Failed to record usage event"
        );
    }
    return null;
};

let cachedTokenMeterEventName = stripeTokenMeterEventName;
let tokenMeterResolved =
    Boolean(stripeTokenMeterEventName) || !stripeTokenMeterId;

const resolveTokenMeterEventName = async () => {
    if (!tokenMeterConfigured) {
        return "";
    }
    if (tokenMeterResolved) {
        return cachedTokenMeterEventName;
    }
    tokenMeterResolved = true;
    if (!stripe || !stripeTokenMeterId) {
        return cachedTokenMeterEventName;
    }
    try {
        const meter = await stripe.billing.meters.retrieve(stripeTokenMeterId);
        cachedTokenMeterEventName = meter?.event_name || "";
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Failed to load Stripe meter"
        );
    }
    return cachedTokenMeterEventName;
};

const normalizeStripeCustomerId = (value) => {
    if (!value) {
        return "";
    }
    if (typeof value === "string") {
        return value;
    }
    if (typeof value === "object" && typeof value.id === "string") {
        return value.id;
    }
    return "";
};

const pickStripeSubscription = (list) => {
    const subscriptions = Array.isArray(list) ? list : [];
    if (subscriptions.length === 0) {
        return null;
    }
    const active = subscriptions.find((subscription) =>
        isBillingActive(subscription?.status)
    );
    return active || subscriptions[0];
};

const refreshTenantBillingFromStripe = async (billing) => {
    if (!stripe || !billing) {
        return billing;
    }
    let subscription = null;
    if (billing.stripeSubscriptionId) {
        try {
            subscription = await stripe.subscriptions.retrieve(
                billing.stripeSubscriptionId
            );
        } catch (err) {
            app.log.warn(
                {
                    err: err.message || err,
                    subscriptionId: billing.stripeSubscriptionId,
                },
                "Failed to refresh Stripe subscription"
            );
        }
    }
    if (!subscription && billing.stripeCustomerId) {
        try {
            const response = await stripe.subscriptions.list({
                customer: billing.stripeCustomerId,
                status: "all",
                limit: 5,
            });
            subscription = pickStripeSubscription(response?.data || []);
        } catch (err) {
            app.log.warn(
                {
                    err: err.message || err,
                    customerId: billing.stripeCustomerId,
                },
                "Failed to list Stripe subscriptions"
            );
        }
    }
    if (!subscription) {
        return billing;
    }
    const items = subscription?.items?.data || [];
    const planKey =
        subscription?.metadata?.plan ||
        resolvePlanFromSubscriptionItems(items);
    const tokenItemId = resolveTokenItemId(items);
    const currentPeriodEnd = subscription?.current_period_end
        ? new Date(subscription.current_period_end * 1000)
        : null;
    const customerId = normalizeStripeCustomerId(subscription.customer);
    const planToApply = planKey || billing.plan || "";
    const limits = planToApply
        ? getPlanLimits(planToApply)
        : {
              repoLimit: billing.repoLimit ?? null,
              tokenLimit: billing.tokenLimit ?? null,
          };

    const updates = {
        plan: planToApply || null,
        subscriptionStatus: subscription.status || null,
        stripeCustomerId: customerId || billing.stripeCustomerId || null,
        stripeSubscriptionId: subscription.id || billing.stripeSubscriptionId,
        stripeTokenItemId: tokenItemId || billing.stripeTokenItemId || null,
        currentPeriodEnd,
        repoLimit: limits.repoLimit ?? null,
        tokenLimit: limits.tokenLimit ?? null,
    };

    await updateTenantBilling(billing.tenantId, updates);
    return { ...billing, ...updates };
};

const reportUsageToStripe = async ({ tenantId, usageEventId, tokens }) => {
    if (!stripe || !tenantId || !usageEventId) {
        return;
    }
    if (typeof tokens !== "number" || !Number.isFinite(tokens)) {
        return;
    }
    try {
        let billing = await fetchTenantBilling(tenantId);
        if (!billing) {
            return;
        }
        let planKey = billing.plan || "";
        let planConfigured = Boolean(planKey && PLAN_DEFINITIONS[planKey]);
        const needsRefresh =
            !planConfigured ||
            !billing.subscriptionStatus ||
            (tokenMeterConfigured
                ? !billing.stripeCustomerId
                : !billing.stripeTokenItemId);
        if (
            needsRefresh &&
            (billing.stripeSubscriptionId || billing.stripeCustomerId)
        ) {
            billing = await refreshTenantBillingFromStripe(billing);
            planKey = billing?.plan || "";
            planConfigured = Boolean(planKey && PLAN_DEFINITIONS[planKey]);
        }
        if (!billing) {
            return;
        }
        const meterReady = tokenMeterConfigured
            ? Boolean(billing.stripeCustomerId)
            : false;
        const itemReady = Boolean(billing.stripeTokenItemId);
        const limits = planConfigured ? getPlanLimits(planKey) : null;
        const tokenUsageEnabled = limits?.tokenUsage || (!limits && (meterReady || itemReady));
        if (!tokenUsageEnabled) {
            return;
        }
        if (
            billing.subscriptionStatus &&
            !isBillingActive(billing.subscriptionStatus)
        ) {
            return;
        }
        const usageTotal = tokenMeterConfigured
            ? await fetchTokenUsageForPeriod(tenantId, getUsagePeriod())
            : tokens;
        const quantity = Math.max(Math.round(usageTotal), 0);
        if (quantity <= 0) {
            return;
        }
        if (tokenMeterConfigured) {
            const eventName = await resolveTokenMeterEventName();
            if (!eventName) {
                app.log.warn(
                    { meterId: stripeTokenMeterId },
                    "Stripe meter event name is not configured"
                );
                return;
            }
            if (!billing.stripeCustomerId) {
                return;
            }
            await stripe.billing.meterEvents.create({
                event_name: eventName,
                payload: {
                    [stripeTokenMeterCustomerKey]: billing.stripeCustomerId,
                    [stripeTokenMeterValueKey]: String(quantity),
                },
                identifier: `usage-${usageEventId}`,
                timestamp: Math.floor(Date.now() / 1000),
            });
            return;
        }
        if (!billing.stripeTokenItemId) {
            return;
        }
        await stripe.subscriptionItems.createUsageRecord(
            billing.stripeTokenItemId,
            {
                quantity,
                timestamp: Math.floor(Date.now() / 1000),
                action: "increment",
            },
            { idempotencyKey: `usage-${usageEventId}` }
        );
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Failed to report usage to Stripe"
        );
    }
};

const formatUsageSnapshotKey = (value = new Date()) => {
    const year = value.getUTCFullYear();
    const month = String(value.getUTCMonth() + 1).padStart(2, "0");
    const day = String(value.getUTCDate()).padStart(2, "0");
    const hour = String(value.getUTCHours()).padStart(2, "0");
    return `${year}${month}${day}${hour}`;
};

const fetchUsageTotalsForPeriod = async (period) => {
    if (!period?.start || !period?.end) {
        return new Map();
    }
    const rows = extractRows(
        await db.execute(sql`
            select tenant_id as "tenantId",
                   coalesce(sum(tokens), 0)::int as "count"
            from ${usageEvents}
            where created_at >= ${period.start}
              and created_at < ${period.end}
            group by tenant_id
        `)
    );
    const totals = new Map();
    for (const row of rows) {
        const tenantId = Number(row.tenantId);
        const count = Number.parseInt(row.count, 10);
        if (!Number.isFinite(tenantId)) {
            continue;
        }
        totals.set(tenantId, Number.isFinite(count) ? count : 0);
    }
    return totals;
};

const syncStripeUsageSnapshots = async (label) => {
    if (!stripe || !tokenMeterConfigured) {
        return;
    }
    if (stripeUsageSyncInFlight) {
        return;
    }
    stripeUsageSyncInFlight = true;
    try {
        const eventName = await resolveTokenMeterEventName();
        if (!eventName) {
            app.log.warn(
                { meterId: stripeTokenMeterId },
                "Stripe usage sync: meter event name is not configured"
            );
            return;
        }
        const period = getUsagePeriod();
        const usageTotals = await fetchUsageTotalsForPeriod(period);
        const tenantRows = await db
            .select({
                tenantId: tenants.id,
                stripeCustomerId: tenants.stripeCustomerId,
                subscriptionStatus: tenants.subscriptionStatus,
            })
            .from(tenants)
            .where(sql`${tenants.stripeCustomerId} is not null`);

        const snapshotKey = formatUsageSnapshotKey(new Date());
        const timestamp = Math.floor(Date.now() / 1000);
        let updated = 0;

        for (const tenant of tenantRows) {
            const stripeCustomerId = normalizeStripeCustomerId(
                tenant.stripeCustomerId
            );
            if (!stripeCustomerId) {
                continue;
            }
            if (
                tenant.subscriptionStatus &&
                !isBillingActive(tenant.subscriptionStatus)
            ) {
                continue;
            }
            const tokens = usageTotals.get(tenant.tenantId) || 0;
            const quantity = Math.max(Math.round(tokens), 0);
            const identifier = `usage-snapshot-${tenant.tenantId}-${snapshotKey}`;
            try {
                await stripe.billing.meterEvents.create({
                    event_name: eventName,
                    payload: {
                        [stripeTokenMeterCustomerKey]: stripeCustomerId,
                        [stripeTokenMeterValueKey]: String(quantity),
                    },
                    identifier,
                    timestamp,
                });
                updated += 1;
            } catch (err) {
                app.log.warn(
                    {
                        err: err.message || err,
                        tenantId: tenant.tenantId,
                    },
                    "Stripe usage sync: failed to post meter event"
                );
            }
        }
        if (updated > 0) {
            app.log.info(
                { label, updated },
                "Stripe usage sync: posted meter events"
            );
        }
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Stripe usage sync failed"
        );
    } finally {
        stripeUsageSyncInFlight = false;
    }
};

const fetchProjectRowsForTenant = async (tenantId) => {
    if (!tenantId) {
        return [];
    }
    return db
        .select({
            id: projects.id,
            tenantId: projects.tenantId,
            name: projects.name,
            repoUrl: projects.repoUrl,
            description: projects.description,
            tags: projects.tags,
            featured: projects.featured,
            category: projects.category,
            createdAt: projects.createdAt,
        })
        .from(projects)
        .where(eq(projects.tenantId, tenantId))
        .orderBy(desc(projects.id));
};

const fetchAllProjectRows = async () =>
    db
        .select({
            id: projects.id,
            tenantId: projects.tenantId,
            name: projects.name,
            repoUrl: projects.repoUrl,
            description: projects.description,
            tags: projects.tags,
            featured: projects.featured,
            category: projects.category,
            createdAt: projects.createdAt,
        })
        .from(projects)
        .orderBy(desc(projects.id));

const fetchProjectsForTenant = async (tenantId) => {
    const rows = await fetchProjectRowsForTenant(tenantId);
    return rows.map(formatProjectRow);
};

const fetchProjectById = async (tenantId, projectId) => {
    if (!tenantId || !projectId) {
        return null;
    }
    const rows = await db
        .select({
            id: projects.id,
            name: projects.name,
            repoUrl: projects.repoUrl,
            description: projects.description,
            tags: projects.tags,
            featured: projects.featured,
            category: projects.category,
        })
        .from(projects)
        .where(and(eq(projects.tenantId, tenantId), eq(projects.id, projectId)))
        .limit(1);
    return rows[0] ? formatProjectRow(rows[0]) : null;
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
        typeof project.repoUrl === "string"
            ? project.repoUrl.trim()
            : typeof project.repo === "string"
            ? project.repo.trim()
            : "";
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

const resolveRepoFromQuestion = (question, projects) => {
    if (typeof question !== "string" || !question.trim()) {
        return null;
    }

    if (!Array.isArray(projects) || projects.length === 0) {
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

const telemetryEventTypes = new Set([
    "page_view",
    "time_on_page",
    "chat_message",
]);

const normalizeTelemetryEventType = (value) => {
    if (typeof value !== "string") {
        return null;
    }
    const trimmed = value.trim().toLowerCase();
    if (!trimmed || !telemetryEventTypes.has(trimmed)) {
        return null;
    }
    return trimmed;
};

const normalizeTelemetryValue = (value) => {
    if (value === null || value === undefined || value === "") {
        return null;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) {
        return null;
    }
    return Math.max(parsed, 0);
};

const normalizeTelemetryMetadata = (value) => {
    if (!value || typeof value !== "object") {
        return null;
    }
    if (Array.isArray(value)) {
        return null;
    }
    return value;
};

const fetchChatSession = async (sessionId, tenantId) => {
    if (!sessionId || !tenantId) {
        return null;
    }
    const rows = await db
        .select({
            id: chatSessions.id,
            tenantId: chatSessions.tenantId,
            visitorId: chatSessions.visitorId,
            createdAt: chatSessions.createdAt,
        })
        .from(chatSessions)
        .where(
            and(eq(chatSessions.id, sessionId), eq(chatSessions.tenantId, tenantId))
        )
        .limit(1);
    return rows[0] || null;
};

const createChatSession = async ({ tenantId, visitorId }) => {
    const result = await db
        .insert(chatSessions)
        .values({ tenantId, visitorId: visitorId || null })
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

const listChatSessions = async ({ tenantId, visitorId, limit }) => {
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
        where s.tenant_id = ${tenantId}
          and s.visitor_id = ${visitorId}
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
                "X-GitHub-Api-Version": "2022-11-28",
            },
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
        (owner && repo
            ? await fetchInstallationIdForRepo(owner, repo)
            : null);
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
                "X-GitHub-Api-Version": "2022-11-28",
            },
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
                ...(fetchOptions.headers || {}),
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
    const result = await fetchGitHubJson(`/repos/${owner}/${repo}`, {
        auth: { owner, repo },
    });
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

const resolveReturnToUrl = (value) => {
    if (!webBaseUrl) {
        return "";
    }
    try {
        const base = new URL(webBaseUrl);
        if (!value || typeof value !== "string") {
            return base.toString();
        }
        const resolved = new URL(value, base);
        if (resolved.origin !== base.origin) {
            return base.toString();
        }
        return resolved.toString();
    } catch {
        return webBaseUrl;
    }
};

const exchangeGitHubCode = async (code) => {
    if (!oauthClientId || !oauthClientSecret) {
        throw new Error("GitHub OAuth credentials are not configured");
    }
    const response = await fetch("https://github.com/login/oauth/access_token", {
        method: "POST",
        headers: {
            Accept: "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
            client_id: oauthClientId,
            client_secret: oauthClientSecret,
            code,
            redirect_uri: oauthRedirectUrl,
        }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.access_token) {
        throw new Error(payload.error_description || "OAuth token exchange failed");
    }
    return payload.access_token;
};

const fetchGitHubUserProfile = async (accessToken) => {
    const result = await fetchGitHubJson("/user", {
        headers: {
            Authorization: `Bearer ${accessToken}`,
        },
    });
    if (result.error) {
        throw new Error(result.error);
    }
    return result.data;
};

const fetchGitHubUserEmails = async (accessToken) => {
    const result = await fetchGitHubJson("/user/emails", {
        headers: {
            Authorization: `Bearer ${accessToken}`,
        },
    });
    if (result.error) {
        return [];
    }
    return Array.isArray(result.data) ? result.data : [];
};

const resolveGitHubEmail = (profile, emails) => {
    const email =
        typeof profile?.email === "string" ? profile.email.trim() : "";
    if (email) {
        return email;
    }
    const list = Array.isArray(emails) ? emails : [];
    const primary = list.find((item) => item?.primary && item?.verified);
    if (primary?.email) {
        return primary.email.trim();
    }
    const verified = list.find((item) => item?.verified);
    if (verified?.email) {
        return verified.email.trim();
    }
    const fallback = list.find((item) => item?.email);
    if (fallback?.email) {
        return fallback.email.trim();
    }
    const login =
        typeof profile?.login === "string" ? profile.login.trim() : "github-user";
    return `${login}@users.noreply.github.com`;
};

const upsertUserFromGitHub = async (profile, emails) => {
    const githubId =
        typeof profile?.id === "number" || typeof profile?.id === "string"
            ? String(profile.id)
            : "";
    const githubUsername =
        typeof profile?.login === "string" ? profile.login.trim() : "";
    const name =
        typeof profile?.name === "string" && profile.name.trim()
            ? profile.name.trim()
            : githubUsername || "GitHub User";
    const email = resolveGitHubEmail(profile, emails).toLowerCase();
    const avatarUrl =
        typeof profile?.avatar_url === "string" ? profile.avatar_url.trim() : "";
    const suggestedHandle = validateHandleInput(githubUsername).handle;

    const existingByGithub = githubId
        ? await db
              .select({ id: users.id, tenantId: users.tenantId })
              .from(users)
              .where(eq(users.githubId, githubId))
              .limit(1)
        : [];
    let existing = existingByGithub[0] || null;
    if (!existing && email) {
        const existingByEmail = await db
            .select({ id: users.id, tenantId: users.tenantId })
            .from(users)
            .where(eq(users.email, email))
            .limit(1);
        existing = existingByEmail[0] || null;
    }

    if (existing) {
        await db
            .update(users)
            .set({
                email,
                name,
                githubId: githubId || null,
                githubUsername: githubUsername || null,
                avatarUrl: avatarUrl || null,
            })
            .where(eq(users.id, existing.id));

        let tenantId = existing.tenantId;
        if (!tenantId) {
            const [tenant] = await db
                .insert(tenants)
                .values({ name })
                .returning({ id: tenants.id });
            tenantId = tenant?.id;
            if (tenantId) {
                await db
                    .update(users)
                    .set({ tenantId })
                    .where(eq(users.id, existing.id));
            }
        }

        if (tenantId && suggestedHandle) {
            const tenantProfile = await fetchTenantProfile(tenantId);
            if (tenantProfile && !tenantProfile.handle) {
                const available = await isHandleAvailable(
                    suggestedHandle,
                    tenantId
                );
                if (available) {
                    await db
                        .update(tenants)
                        .set({ handle: suggestedHandle })
                        .where(eq(tenants.id, tenantId));
                }
            }
        }

        return { userId: existing.id, tenantId };
    }

    let initialHandle = null;
    if (suggestedHandle) {
        const available = await isHandleAvailable(suggestedHandle, null);
        if (available) {
            initialHandle = suggestedHandle;
        }
    }

    const [tenant] = await db
        .insert(tenants)
        .values({ name, handle: initialHandle })
        .returning({ id: tenants.id });
    const tenantId = tenant?.id;
    const [user] = await db
        .insert(users)
        .values({
            tenantId,
            email,
            name,
            githubId: githubId || null,
            githubUsername: githubUsername || null,
            avatarUrl: avatarUrl || null,
        })
        .returning({ id: users.id });
    return { userId: user?.id, tenantId };
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
    const category = (() => {
        if (typeof body?.category === "string" && body.category.trim()) {
            return body.category.trim();
        }
        if (Array.isArray(body?.categories)) {
            const match = body.categories.find(
                (value) => typeof value === "string" && value.trim()
            );
            return match ? match.trim() : "";
        }
        return "";
    })();
    const project = {
        name,
        repoUrl: canonicalRepo,
        description,
        tags,
        featured,
    };

    if (category) {
        project.category = category;
    }

    return { project };
};

const fetchUsageSummary = async (tenantId) => {
    if (!tenantId) {
        return {
            repoCount: 0,
            sessionCount: 0,
            messageCount: 0,
            tokenCount: 0,
        };
    }
    const parseCount = (value) => {
        const numeric = Number.parseInt(value, 10);
        return Number.isFinite(numeric) ? numeric : 0;
    };

    const period = getUsagePeriod();
    const repoCount = await fetchRepoCount(tenantId);
    const sessionRows = extractRows(
        await db.execute(sql`
            select count(*)::int as "count"
            from ${chatSessions}
            where tenant_id = ${tenantId}
              and created_at >= ${period.start}
              and created_at < ${period.end}
        `)
    );
    const messageRows = extractRows(
        await db.execute(sql`
            select count(*)::int as "count"
            from ${chatMessages} m
            join ${chatSessions} s on s.id = m.session_id
            where s.tenant_id = ${tenantId}
              and m.created_at >= ${period.start}
              and m.created_at < ${period.end}
        `)
    );
    const tokenCount = await fetchTokenUsageForPeriod(tenantId, period);

    return {
        repoCount,
        sessionCount: parseCount(sessionRows[0]?.count),
        messageCount: parseCount(messageRows[0]?.count),
        tokenCount,
        periodStart: period.start.toISOString(),
    };
};

const fetchLatestIngestJob = async (projectId, projectRepo) => {
    if (!projectId && !projectRepo) {
        return null;
    }
    const predicate = projectId
        ? eq(ingestJobs.projectId, projectId)
        : eq(ingestJobs.projectRepo, projectRepo);
    const rows = await db
        .select({
            status: ingestJobs.status,
            createdAt: ingestJobs.createdAt,
            updatedAt: ingestJobs.updatedAt,
            finishedAt: ingestJobs.finishedAt,
        })
        .from(ingestJobs)
        .where(predicate)
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

const hasSourcesForProject = async (projectId) => {
    if (!projectId) {
        return false;
    }
    const rows = await db
        .select({ id: sources.id })
        .from(sources)
        .where(eq(sources.projectId, projectId))
        .limit(1);
    return rows.length > 0;
};

const enqueueReindexForAllProjects = async () => {
    const projectRows = await fetchAllProjectRows();
    const projects = projectRows.filter(
        (project) => project && project.repoUrl
    );
    if (projects.length === 0) {
        return { enqueued: 0 };
    }

    const now = Date.now();
    const toEnqueue = [];
    let skippedActive = 0;
    let skippedRecent = 0;

    for (const project of projects) {
        const latestJob = await fetchLatestIngestJob(
            project.id,
            project.repoUrl
        );
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
    const projectRows = await fetchAllProjectRows();
    const projects = projectRows.filter(
        (project) => project && project.repoUrl
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
        const hasSources = await hasSourcesForProject(project.id);
        if (hasSources) {
            skippedIndexed += 1;
            continue;
        }
        const latestJob = await fetchLatestIngestJob(
            project.id,
            project.repoUrl
        );
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
            projectId: ingestJobs.projectId,
            projectRepo: ingestJobs.projectRepo,
            projectName: ingestJobs.projectName,
            tenantId: projects.tenantId,
        })
        .from(ingestJobs)
        .leftJoin(projects, eq(projects.id, ingestJobs.projectId))
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
                        lastMessage: "Canceled by request",
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
                        projectId: row.projectId || null,
                        tenantId: row.tenantId || null,
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

const retrieveChunks = async (question, repoFilter, limit, tenantId) => {
    if (!openai) {
        throw new Error("OPENAI_API_KEY is not set");
    }
    if (!tenantId) {
        return [];
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
    join ${projects} p on p.id = s.project_id
  `;

    const clauses = [sql`p.tenant_id = ${tenantId}`];
    if (repoFilter) {
        clauses.push(
            sql`s.repo_owner = ${repoFilter.owner}`,
            sql`s.repo_name = ${repoFilter.repo}`
        );
    }

    query = sql`${query}
    where ${sql.join(clauses, sql` and `)}
    order by c.embedding <=> ${vector}::vector
    limit ${limit}
  `;

    const result = await db.execute(query);
    return extractRows(result);
};

const retrieveLexicalChunks = async (keywords, repoFilter, limit, tenantId) => {
    if (!Array.isArray(keywords) || keywords.length === 0) {
        return [];
    }
    if (!tenantId) {
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
      join ${projects} p on p.id = s.project_id
    `;

    const clauses = [sql`p.tenant_id = ${tenantId}`];
    if (repoFilter) {
        clauses.push(
            sql`s.repo_owner = ${repoFilter.owner}`,
            sql`s.repo_name = ${repoFilter.repo}`
        );
    }
    clauses.push(keywordClause);

    query = sql`${query}
      where ${sql.join(clauses, sql` and `)}
      order by c.id desc
      limit ${limit}
    `;

    const result = await db.execute(query);
    return extractRows(result);
};

const retrieveEntryPointChunks = async (repoFilter, limit, tenantId) => {
    if (!tenantId) {
        return [];
    }
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
      join ${projects} p on p.id = s.project_id
    `;

    const clauses = [
        pathClause,
        sql`(c.metadata->>'chunkIndex')::int in (0, 1)`,
        sql`p.tenant_id = ${tenantId}`,
    ];
    if (repoFilter) {
        clauses.push(
            sql`s.repo_owner = ${repoFilter.owner}`,
            sql`s.repo_name = ${repoFilter.repo}`
        );
    }

    query = sql`${query}
      where ${sql.join(clauses, sql` and `)}
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
    tenantId,
    skipSemantic = false,
}) => {
    if (skipSemantic) {
        return [];
    }

    let keywords = extractKeywords(question);
    const baseRows = await retrieveChunks(
        retrievalQuestion,
        repoFilter,
        limit,
        tenantId
    );
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
        limit,
        tenantId
    );
    let merged = mergeRows(baseRows, lexicalRows, chatMaxContextChunks);

    if (isEntryPointQuestion(question)) {
        const entryRows = await retrieveEntryPointChunks(
            repoFilter,
            Math.max(limit, 6),
            tenantId
        );
        merged = mergeRows(merged, entryRows, chatMaxContextChunks);
    }

    if (allowGlobalFallback && merged.length < limit) {
        const globalBase = await retrieveChunks(
            retrievalQuestion,
            null,
            limit,
            tenantId
        );
        const globalLexical = await retrieveLexicalChunks(
            keywords,
            null,
            limit,
            tenantId
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

const fetchRepoStats = async (limit, tenantId) => {
    if (!tenantId) {
        return [];
    }
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
        join ${projects} p on p.id = s.project_id
        where p.tenant_id = ${tenantId}
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

const requireAuth = async (request, reply) => {
    const session = await getAuthSession(request);
    if (!session) {
        reply.code(401).send({ error: "Not authenticated" });
        return null;
    }
    return session;
};

const resolveTenantContext = async (request, { allowPublic = false } = {}) => {
    const handleInput =
        request.query?.handle ||
        request.query?.tenant ||
        request.body?.handle ||
        request.body?.tenant ||
        request.headers["x-tenant-handle"];
    const normalizedHandle = normalizeHandle(handleInput);
    const session = await getAuthSession(request);
    if (allowPublic && normalizedHandle) {
        const tenant = await resolveTenantByHandle(normalizedHandle);
        if (!tenant) {
            return null;
        }
        const isOwner = Boolean(
            session && session.tenantId === tenant.tenantId
        );
        if (!tenant.isPublic && !isOwner) {
            return null;
        }
        return {
            tenantId: tenant.tenantId,
            owner: tenant,
            user: session?.user,
            isOwner,
        };
    }
    if (session) {
        return {
            tenantId: session.tenantId,
            user: session.user,
            isOwner: true,
        };
    }
    if (!allowPublic) {
        return null;
    }
    return null;
};

const refreshProjectDescriptions = async (projectRows) => {
    if (!Array.isArray(projectRows) || projectRows.length === 0) {
        return { updated: 0, checked: 0 };
    }

    const targets = [];
    for (const project of projectRows) {
        const repo = parseRepoFromProject(project);
        if (!repo) {
            continue;
        }
        const key = `${repo.owner}/${repo.repo}`.toLowerCase();
        targets.push({ repo, key, projectId: project.id });
    }
    if (targets.length === 0) {
        return { updated: 0, checked: 0 };
    }

    const descriptionsByRepo = new Map();
    for (const target of targets) {
        const result = await fetchRepoMetadata(
            target.repo.owner,
            target.repo.repo
        );
        if (result?.error) {
            continue;
        }
        const description =
            typeof result?.data?.description === "string"
                ? result.data.description.trim()
                : "";
        descriptionsByRepo.set(target.projectId, description);
    }

    if (descriptionsByRepo.size === 0) {
        return { updated: 0, checked: targets.length };
    }

    let updatedCount = 0;
    for (const project of projectRows) {
        if (!descriptionsByRepo.has(project.id)) {
            continue;
        }
        const nextDescription = descriptionsByRepo.get(project.id) || "";
        const currentDescription =
            typeof project.description === "string" ? project.description : "";
        if (currentDescription === nextDescription) {
            continue;
        }
        updatedCount += 1;
        await db
            .update(projects)
            .set({ description: nextDescription })
            .where(eq(projects.id, project.id));
    }

    return { updated: updatedCount, checked: targets.length };
};

const enqueueIngestJobs = async (projectRows) => {
    if (!Array.isArray(projectRows) || projectRows.length === 0) {
        return [];
    }
    try {
        await refreshProjectDescriptions(projectRows);
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Failed to refresh project descriptions"
        );
    }
    const now = new Date();
    const jobRows = projectRows.map((project) => ({
        projectId: project.id,
        projectRepo: project.repoUrl || project.repo,
        projectName: project.name || project.repoUrl || project.repo,
        status: "queued",
        createdAt: now,
    }));

    const inserted = await db.insert(ingestJobs).values(jobRows).returning({
        id: ingestJobs.id,
        projectRepo: ingestJobs.projectRepo,
        projectName: ingestJobs.projectName,
        projectId: ingestJobs.projectId,
    });

    const enqueued = [];
    for (const jobRecord of inserted) {
        const projectRow = projectRows.find(
            (project) => project.id === jobRecord.projectId
        );
        const job = await ingestQueue.add(
            JOB_TYPES.ingestRepoDocs,
            {
                ingestJobId: jobRecord.id,
                repo: jobRecord.projectRepo,
                name: jobRecord.projectName,
                projectId: jobRecord.projectId,
                tenantId: projectRow?.tenantId || null,
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

app.get("/healthz", async () => ({ ok: true }));

app.get("/auth/github/start", async (request, reply) => {
    if (!oauthClientId || !oauthRedirectUrl) {
        reply.code(500).send({ error: "GitHub OAuth is not configured" });
        return;
    }
    const state = crypto.randomBytes(16).toString("hex");
    const returnTo = resolveReturnToUrl(request.query?.returnTo);
    setCookie(reply, OAUTH_STATE_COOKIE, state, {
        maxAge: 300,
        httpOnly: true,
        sameSite: "Lax",
        secure: cookieSecure,
        domain: cookieDomain || undefined,
    });
    if (returnTo) {
        setCookie(reply, OAUTH_RETURN_COOKIE, returnTo, {
            maxAge: 600,
            httpOnly: true,
            sameSite: "Lax",
            secure: cookieSecure,
            domain: cookieDomain || undefined,
        });
    }
    const params = new URLSearchParams({
        client_id: oauthClientId,
        redirect_uri: oauthRedirectUrl,
        scope: "read:user user:email",
        state,
    });
    reply.redirect(`https://github.com/login/oauth/authorize?${params.toString()}`);
});

app.get("/auth/github/callback", async (request, reply) => {
    const code =
        typeof request.query?.code === "string" ? request.query.code : "";
    const state =
        typeof request.query?.state === "string" ? request.query.state : "";
    const storedState = getCookie(request, OAUTH_STATE_COOKIE);
    const returnTo = getCookie(request, OAUTH_RETURN_COOKIE) || webBaseUrl || "/";
    clearCookie(reply, OAUTH_STATE_COOKIE);
    clearCookie(reply, OAUTH_RETURN_COOKIE);

    if (!code || !state || !storedState || state !== storedState) {
        reply.code(400).send({ error: "OAuth state mismatch" });
        return;
    }

    try {
        const accessToken = await exchangeGitHubCode(code);
        const profile = await fetchGitHubUserProfile(accessToken);
        const emails = await fetchGitHubUserEmails(accessToken);
        const { userId, tenantId } = await upsertUserFromGitHub(profile, emails);
        if (!userId || !tenantId) {
            reply.code(500).send({ error: "Failed to create user account" });
            return;
        }
        const session = await createAuthSession({ userId, tenantId });
        setCookie(reply, SESSION_COOKIE_NAME, session.token, {
            expires: session.expiresAt,
            maxAge: Math.floor(authSessionTtlMs / 1000),
            httpOnly: true,
            sameSite: "Lax",
            secure: cookieSecure,
            domain: cookieDomain || undefined,
        });
        reply.redirect(returnTo);
    } catch (err) {
        reply.code(500).send({ error: err.message || "OAuth failed" });
    }
});

app.get("/auth/me", async (request, reply) => {
    const session = await getAuthSession(request);
    if (!session) {
        reply.code(401).send({ error: "Not authenticated" });
        return;
    }
    reply.send({
        user: session.user,
        tenantId: session.tenantId,
    });
});

app.post("/auth/logout", async (request, reply) => {
    const rawToken = getCookie(request, SESSION_COOKIE_NAME);
    if (rawToken) {
        const tokenHash = hashToken(rawToken);
        await db.delete(authSessions).where(eq(authSessions.tokenHash, tokenHash));
    }
    clearCookie(reply, SESSION_COOKIE_NAME);
    reply.send({ status: "ok" });
});

app.get("/account/usage", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    try {
        const summary = await fetchUsageSummary(session.tenantId);
        reply.send({ summary });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load usage summary" });
    }
});

app.get("/account/profile", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    try {
        const profile = await fetchTenantProfile(session.tenantId);
        if (!profile) {
            reply.code(404).send({ error: "Profile not found" });
            return;
        }
        reply.send({
            profile: {
                handle: profile.handle || "",
                bio: profile.bio || "",
                isPublic:
                    typeof profile.isPublic === "boolean"
                        ? profile.isPublic
                        : true,
                name:
                    profile.userName ||
                    profile.tenantName ||
                    profile.githubUsername ||
                    "",
                githubUsername: profile.githubUsername || "",
                avatarUrl: profile.avatarUrl || "",
            },
        });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load profile" });
    }
});

app.post("/account/profile", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    const body = request.body || {};
    try {
        const current = await fetchTenantProfile(session.tenantId);
        if (!current) {
            reply.code(404).send({ error: "Profile not found" });
            return;
        }
        const updates = {};

        if (Object.prototype.hasOwnProperty.call(body, "handle")) {
            const { handle, error } = validateHandleInput(body.handle);
            if (error) {
                reply.code(400).send({ error });
                return;
            }
            if (handle) {
                const available = await isHandleAvailable(
                    handle,
                    session.tenantId
                );
                if (!available) {
                    reply
                        .code(409)
                        .send({ error: "Handle is already taken." });
                    return;
                }
                updates.handle = handle;
            } else {
                updates.handle = null;
            }
        }

        if (Object.prototype.hasOwnProperty.call(body, "bio")) {
            if (typeof body.bio !== "string" && body.bio !== null) {
                reply.code(400).send({ error: "Bio must be a string." });
                return;
            }
            const trimmed = typeof body.bio === "string" ? body.bio.trim() : "";
            if (trimmed.length > MAX_BIO_LENGTH) {
                reply.code(400).send({
                    error: `Bio must be ${MAX_BIO_LENGTH} characters or fewer.`,
                });
                return;
            }
            updates.bio = trimmed ? normalizeBioInput(trimmed) : null;
        }

        if (Object.prototype.hasOwnProperty.call(body, "isPublic")) {
            const nextValue = normalizeBooleanInput(
                body.isPublic,
                typeof current.isPublic === "boolean" ? current.isPublic : true
            );
            if (typeof nextValue !== "boolean") {
                reply
                    .code(400)
                    .send({ error: "isPublic must be true or false." });
                return;
            }
            updates.isPublic = nextValue;
        }

        if (Object.keys(updates).length > 0) {
            await db
                .update(tenants)
                .set(updates)
                .where(eq(tenants.id, session.tenantId));
        }

        const updated = await fetchTenantProfile(session.tenantId);
        reply.send({
            profile: {
                handle: updated?.handle || "",
                bio: updated?.bio || "",
                isPublic:
                    typeof updated?.isPublic === "boolean"
                        ? updated.isPublic
                        : true,
                name:
                    updated?.userName ||
                    updated?.tenantName ||
                    updated?.githubUsername ||
                    "",
                githubUsername: updated?.githubUsername || "",
                avatarUrl: updated?.avatarUrl || "",
            },
        });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to update profile" });
    }
});

app.get("/account/limits", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    try {
        const limits = await fetchTenantLimits(session.tenantId);
        reply.send({
            limits: {
                tokenLimit: limits.tokenLimit,
            },
        });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load limits" });
    }
});

app.post("/account/limits", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    if (billingEnabled) {
        reply.code(403).send({ error: "Limits are managed by billing." });
        return;
    }
    const body = request.body || {};

    const parseLimit = (value, label) => {
        if (value === undefined) {
            return { skip: true };
        }
        if (value === null || value === "") {
            return { value: null };
        }
        const parsed =
            typeof value === "number"
                ? Math.trunc(value)
                : Number.parseInt(value, 10);
        if (!Number.isFinite(parsed) || parsed < 0) {
            return {
                error: `${label} must be a whole number or left blank.`,
            };
        }
        return { value: parsed };
    };

    try {
        const updates = {};
        const tokenResult = parseLimit(body.tokenLimit, "Token limit");
        if (tokenResult.error) {
            reply.code(400).send({ error: tokenResult.error });
            return;
        }
        if (!tokenResult.skip) {
            updates.tokenLimit = tokenResult.value;
        }

        if (Object.keys(updates).length > 0) {
            await db
                .update(tenants)
                .set(updates)
                .where(eq(tenants.id, session.tenantId));
        }

        const limits = await fetchTenantLimits(session.tenantId);
        reply.send({
            limits: {
                tokenLimit: limits.tokenLimit,
            },
        });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to update limits" });
    }
});

app.get("/account/billing", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    try {
        const billing = await fetchTenantBilling(session.tenantId);
        const planKey = billing?.plan || "";
        const plan = PLAN_DEFINITIONS[planKey] || null;
        reply.send({
            billing: {
                plan: planKey,
                planLabel: plan?.label || "",
                status: billing?.subscriptionStatus || "inactive",
                currentPeriodEnd: billing?.currentPeriodEnd
                    ? new Date(billing.currentPeriodEnd).toISOString()
                    : null,
                repoLimit: billing?.repoLimit ?? null,
                tokenLimit: billing?.tokenLimit ?? null,
                tokenUsage: plan?.tokenUsage || false,
                hasCustomer: Boolean(billing?.stripeCustomerId),
                billingEnabled,
                unlimitedTokenLimit,
            },
        });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to load billing details" });
    }
});

app.post("/billing/checkout", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    if (!billingEnabled) {
        reply.code(500).send({ error: "Stripe is not configured." });
        return;
    }
    if (!stripeCheckoutSuccessUrl || !stripeCheckoutCancelUrl) {
        reply.code(500).send({ error: "Checkout URLs are not configured." });
        return;
    }
    const planKey = normalizePlanInput(request.body?.plan);
    const plan = PLAN_DEFINITIONS[planKey];
    if (!plan || !plan.priceId) {
        reply.code(400).send({ error: "Invalid plan selection." });
        return;
    }
    if (plan.tokenUsage && !stripePriceTokens) {
        reply
            .code(500)
            .send({ error: "Token usage price is not configured." });
        return;
    }

    try {
        const billing = await fetchTenantBilling(session.tenantId);
        const customerEmail = session.user?.email || undefined;
        let stripeCustomerId = billing?.stripeCustomerId || "";
        if (!stripeCustomerId) {
            const customer = await stripe.customers.create({
                email: customerEmail,
                name: session.user?.name || customerEmail || undefined,
                metadata: {
                    tenantId: String(session.tenantId),
                },
            });
            stripeCustomerId = customer.id;
            await updateTenantBilling(session.tenantId, {
                stripeCustomerId,
                billingEmail: customerEmail || null,
            });
        }

        const lineItems = [{ price: plan.priceId, quantity: 1 }];
        if (plan.tokenUsage && stripePriceTokens) {
            lineItems.push({ price: stripePriceTokens });
        }

        const checkoutSession = await stripe.checkout.sessions.create({
            mode: "subscription",
            customer: stripeCustomerId,
            line_items: lineItems,
            success_url: stripeCheckoutSuccessUrl,
            cancel_url: stripeCheckoutCancelUrl,
            allow_promotion_codes: true,
            client_reference_id: String(session.tenantId),
            metadata: {
                tenantId: String(session.tenantId),
                plan: planKey,
            },
            subscription_data: {
                metadata: {
                    tenantId: String(session.tenantId),
                    plan: planKey,
                },
            },
        });

        reply.send({ url: checkoutSession.url });
    } catch (err) {
        reply.code(500).send({
            error: err.message || "Failed to create checkout session",
        });
    }
});

app.post("/billing/portal", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    if (!billingEnabled) {
        reply.code(500).send({ error: "Stripe is not configured." });
        return;
    }
    if (!stripePortalReturnUrl) {
        reply.code(500).send({ error: "Portal return URL is not configured." });
        return;
    }
    try {
        const billing = await fetchTenantBilling(session.tenantId);
        if (!billing?.stripeCustomerId) {
            reply.code(400).send({ error: "No Stripe customer on file." });
            return;
        }
        const portal = await stripe.billingPortal.sessions.create({
            customer: billing.stripeCustomerId,
            return_url: stripePortalReturnUrl,
        });
        reply.send({ url: portal.url });
    } catch (err) {
        reply.code(500).send({
            error: err.message || "Failed to open billing portal",
        });
    }
});

app.post("/webhooks/stripe", async (request, reply) => {
    if (!billingEnabled || !stripeWebhookSecret) {
        reply.code(500).send({ error: "Stripe webhook not configured." });
        return;
    }
    const signature = request.headers["stripe-signature"];
    if (!signature || typeof signature !== "string") {
        reply.code(400).send({ error: "Missing Stripe signature." });
        return;
    }
    let event;
    try {
        event = stripe.webhooks.constructEvent(
            request.rawBody || "",
            signature,
            stripeWebhookSecret
        );
    } catch (err) {
        reply.code(400).send({ error: `Webhook error: ${err.message}` });
        return;
    }

    const handleSubscriptionUpdate = async (subscription) => {
        const items = subscription?.items?.data || [];
        const planKey =
            subscription?.metadata?.plan ||
            resolvePlanFromSubscriptionItems(items);
        const resolvedPlan = planKey || "";
        const tokenItemId = resolveTokenItemId(items);
        const currentPeriodEnd = subscription?.current_period_end
            ? new Date(subscription.current_period_end * 1000)
            : null;
        const tenantId =
            subscription?.metadata?.tenantId ||
            subscription?.metadata?.tenant ||
            null;
        let tenant =
            tenantId && Number.isFinite(Number(tenantId))
                ? await fetchTenantBilling(Number(tenantId))
                : null;
        if (!tenant) {
            tenant = await fetchTenantByStripeSubscriptionId(subscription.id);
        }
        if (!tenant) {
            tenant = await fetchTenantByStripeCustomerId(subscription.customer);
        }
        if (!tenant) {
            app.log.warn(
                { subscriptionId: subscription.id },
                "Stripe webhook: tenant not found"
            );
            return;
        }

        const planToApply = resolvedPlan || tenant.plan || "";
        const limits = planToApply
            ? getPlanLimits(planToApply)
            : {
                  repoLimit: tenant.repoLimit ?? null,
                  tokenLimit: tenant.tokenLimit ?? null,
              };

        await updateTenantBilling(tenant.tenantId, {
            plan: planToApply || null,
            subscriptionStatus: subscription.status || null,
            stripeCustomerId: subscription.customer || tenant.stripeCustomerId,
            stripeSubscriptionId: subscription.id || tenant.stripeSubscriptionId,
            stripeTokenItemId: tokenItemId || null,
            currentPeriodEnd,
            repoLimit: limits.repoLimit ?? null,
            tokenLimit: limits.tokenLimit ?? null,
        });
    };

    const handleCheckoutCompleted = async (session) => {
        const tenantId =
            session?.metadata?.tenantId ||
            session?.client_reference_id ||
            null;
        if (!tenantId) {
            return;
        }
        const numericTenantId = Number(tenantId);
        if (!Number.isFinite(numericTenantId)) {
            return;
        }
        await updateTenantBilling(numericTenantId, {
            stripeCustomerId: session.customer || null,
            stripeSubscriptionId: session.subscription || null,
            billingEmail: session.customer_details?.email || null,
        });
    };

    const handleInvoicePaid = async (invoice) => {
        const subscriptionId = invoice?.subscription || null;
        if (!subscriptionId) {
            return;
        }
        try {
            const subscription = await stripe.subscriptions.retrieve(
                subscriptionId
            );
            await handleSubscriptionUpdate(subscription);
        } catch (err) {
            app.log.warn(
                { err: err.message || err, subscriptionId },
                "Stripe webhook: failed to refresh subscription after invoice"
            );
        }
    };

    try {
        switch (event.type) {
            case "checkout.session.completed":
                await handleCheckoutCompleted(event.data.object);
                break;
            case "customer.subscription.created":
            case "customer.subscription.updated":
            case "customer.subscription.deleted":
                await handleSubscriptionUpdate(event.data.object);
                break;
            case "invoice.payment_failed":
                break;
            case "invoice.paid":
            case "invoice.payment_succeeded":
                await handleInvoicePaid(event.data.object);
                break;
            default:
                break;
        }
    } catch (err) {
        app.log.error(
            { err: err.message || err, eventType: event.type },
            "Stripe webhook failed"
        );
        reply.code(500).send({ error: "Webhook handler failed." });
        return;
    }

    reply.send({ received: true });
});

app.get("/projects", async (request, reply) => {
    const handle = normalizeHandle(request.query?.handle);
    const context = await resolveTenantContext(request, { allowPublic: true });
    if (!context) {
        reply
            .code(handle ? 404 : 401)
            .send({ error: handle ? "Showcase not found" : "Not authenticated" });
        return;
    }
    const projects = await fetchProjectsForTenant(context.tenantId);
    let owner = null;
    if (context.owner) {
        owner = context.owner;
    } else {
        const profile = await fetchTenantProfile(context.tenantId);
        owner = profile ? buildOwnerPayload(profile) : null;
        if (!owner && context.user) {
            owner = {
                handle: context.user.githubUsername || "",
                name: context.user.name || context.user.email || "",
                avatarUrl: context.user.avatarUrl || "",
                bio: "",
                isPublic: true,
            };
        }
    }
    reply.send({ projects, owner });
});

app.post("/projects", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    const hasAccess = await requireActiveSubscription(session.tenantId, reply);
    if (!hasAccess) {
        return;
    }
    const { project, error } = await normalizeProjectInput(request.body);
    if (error) {
        reply.code(400).send({ error });
        return;
    }

    const existing = await db
        .select({ id: projects.id })
        .from(projects)
        .where(
            and(
                eq(projects.tenantId, session.tenantId),
                eq(projects.repoUrl, project.repoUrl)
            )
        )
        .limit(1);

    if (existing.length > 0) {
        reply.code(200).send({ status: "exists" });
        return;
    }

    const limits = await fetchTenantLimits(session.tenantId);
    if (Number.isFinite(limits.repoLimit)) {
        const repoCount = await fetchRepoCount(session.tenantId);
        if (repoCount >= limits.repoLimit) {
            reply.code(403).send({ error: "Repo limit reached." });
            return;
        }
    }

    const [created] = await db
        .insert(projects)
        .values({ ...project, tenantId: session.tenantId })
        .returning({
            id: projects.id,
            name: projects.name,
            repoUrl: projects.repoUrl,
            description: projects.description,
            tags: projects.tags,
            featured: projects.featured,
            category: projects.category,
        });

    let ingestJob = null;
    let ingestError = null;
    try {
        const enqueued = await enqueueIngestJobs([
            { ...created, tenantId: session.tenantId },
        ]);
        ingestJob = enqueued[0] || null;
    } catch (err) {
        ingestError = err.message || "Failed to enqueue ingest job";
    }
    reply.code(201).send({
        status: "created",
        project: formatProjectRow(created),
        ingestJob,
        ingestError,
    });
});

app.post("/projects/:id/category", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    const projectId = normalizeSessionId(request.params?.id);
    if (!projectId) {
        reply.code(400).send({ error: "project id is required" });
        return;
    }
    const rawCategory =
        request.body?.category ??
        request.body?.value ??
        request.body?.name ??
        "";
    const category = normalizeCategoryInput(rawCategory);
    if (category === null) {
        reply.code(400).send({ error: "category must be a string" });
        return;
    }

    const [updated] = await db
        .update(projects)
        .set({ category: category ? category : null })
        .where(
            and(eq(projects.tenantId, session.tenantId), eq(projects.id, projectId))
        )
        .returning({
            id: projects.id,
            name: projects.name,
            repoUrl: projects.repoUrl,
            description: projects.description,
            tags: projects.tags,
            featured: projects.featured,
            category: projects.category,
        });

    if (!updated) {
        reply.code(404).send({ error: "Project not found" });
        return;
    }

    reply.send({ project: formatProjectRow(updated) });
});

app.post("/telemetry", async (request, reply) => {
    const allowedOrigin = resolveCorsOrigin(request);
    if (allowedOrigin) {
        reply.header("Access-Control-Allow-Origin", allowedOrigin);
        reply.header("Access-Control-Allow-Credentials", "true");
        reply.header("Vary", "Origin");
    }

    const visitorId = normalizeVisitorId(
        request.body?.visitorId || request.headers["x-visitor-id"]
    );
    if (!visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }

    const eventType = normalizeTelemetryEventType(
        request.body?.eventType || request.body?.event_type
    );
    if (!eventType) {
        reply.code(400).send({ error: "eventType is required" });
        return;
    }

    const sessionId = normalizeSessionId(
        request.body?.sessionId || request.body?.session_id
    );
    const value = normalizeTelemetryValue(request.body?.value);
    const metadata = normalizeTelemetryMetadata(request.body?.metadata);

    try {
        await db.insert(telemetryEvents).values({
            visitorId,
            sessionId,
            eventType,
            value,
            metadata,
        });
        reply.send({ status: "ok" });
    } catch (err) {
        reply
            .code(500)
            .send({ error: err.message || "Failed to record telemetry" });
    }
});

app.delete("/projects/:id", async (request, reply) => {
    const session = await requireAuth(request, reply);
    if (!session) {
        return;
    }
    const projectId = normalizeSessionId(request.params?.id);
    if (!projectId) {
        reply.code(400).send({ error: "project id is required" });
        return;
    }

    const project = await fetchProjectById(session.tenantId, projectId);
    if (!project) {
        reply.code(404).send({ error: "Project not found" });
        return;
    }

    const sourceRows = await db
        .select({ id: sources.id })
        .from(sources)
        .where(eq(sources.projectId, projectId));
    if (sourceRows.length > 0) {
        const ids = sourceRows.map((row) => row.id);
        await db.delete(chunks).where(inArray(chunks.sourceId, ids));
        await db.delete(sources).where(inArray(sources.id, ids));
    }
    await db
        .delete(projects)
        .where(
            and(eq(projects.tenantId, session.tenantId), eq(projects.id, projectId))
        );
    reply.send({ status: "deleted", project });
});

app.post("/chat/sessions", async (request, reply) => {
    const visitorId = normalizeVisitorId(
        request.body?.visitorId || request.headers["x-visitor-id"]
    );
    if (!visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }

    const context = await resolveTenantContext(request, { allowPublic: true });
    if (!context) {
        reply.code(400).send({ error: "tenant handle is required" });
        return;
    }

    try {
        const sessionId = await createChatSession({
            tenantId: context.tenantId,
            visitorId,
        });
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
    const context = await resolveTenantContext(request, { allowPublic: true });
    if (!context) {
        reply.code(400).send({ error: "tenant handle is required" });
        return;
    }
    await maybePurgeExpiredChatSessions();

    const rawLimit = Number.parseInt(request.query?.limit, 10);
    const limit = Number.isFinite(rawLimit)
        ? Math.min(Math.max(rawLimit, 1), 200)
        : 50;

    try {
        const sessions = await listChatSessions({
            tenantId: context.tenantId,
            visitorId,
            limit,
        });
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
    const context = await resolveTenantContext(request, { allowPublic: true });
    if (!context) {
        reply.code(400).send({ error: "tenant handle is required" });
        return;
    }
    await maybePurgeExpiredChatSessions();

    const session = await fetchChatSession(sessionId, context.tenantId);
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
    const sessionId = normalizeSessionId(request.params?.id);
    if (!sessionId) {
        reply.code(400).send({ error: "sessionId is required" });
        return;
    }

    const visitorId = normalizeVisitorId(
        request.query?.visitorId ||
            request.headers["x-visitor-id"] ||
            request.body?.visitorId
    );
    const context = await resolveTenantContext(request, { allowPublic: true });
    if (!context) {
        reply.code(400).send({ error: "tenant handle is required" });
        return;
    }
    if (!context.isOwner && !visitorId) {
        reply.code(400).send({ error: "visitorId is required" });
        return;
    }

    const session = await fetchChatSession(sessionId, context.tenantId);
    const visitorMismatch =
        !context.isOwner &&
        visitorId &&
        session?.visitorId &&
        session.visitorId !== visitorId;
    if (!session || visitorMismatch) {
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

    const handle = normalizeHandle(
        body.handle ||
            body.tenant ||
            _request.query?.handle ||
            _request.headers["x-tenant-handle"]
    );
    const context = await resolveTenantContext(_request, { allowPublic: true });
    if (!context) {
        reply
            .code(handle ? 404 : 401)
            .send({ error: handle ? "Showcase not found" : "Not authenticated" });
        return;
    }

    const publicSubscriptionMessage =
        "Chat is unavailable because this showcase owner does not have an active subscription.";
    const hasAccess = await requireActiveSubscription(context.tenantId, reply, {
        message: context.isOwner ? undefined : publicSubscriptionMessage,
    });
    if (!hasAccess) {
        return;
    }

    const limits = await fetchTenantLimits(context.tenantId);
    if (Number.isFinite(limits.tokenLimit)) {
        const period = getUsagePeriod();
        const tokenUsage = await fetchTokenUsageForPeriod(
            context.tenantId,
            period
        );
        if (tokenUsage >= limits.tokenLimit) {
            reply.code(429).send({
                error: "Monthly chat token limit reached.",
            });
            return;
        }
    }

    const statsQuestion = isStatsQuestion(question);
    const visitorId = normalizeVisitorId(
        body.visitorId || _request.headers["x-visitor-id"]
    );
    const sessionIdInput =
        body.sessionId || body.session_id || body.chatSessionId;
    let sessionId = normalizeSessionId(sessionIdInput);

    if (sessionId) {
        const session = await fetchChatSession(sessionId, context.tenantId);
        const visitorMismatch =
            !context.isOwner &&
            visitorId &&
            session?.visitorId &&
            session.visitorId !== visitorId;
        if (!session || visitorMismatch) {
            reply.code(404).send({ error: "Session not found" });
            return;
        }
    }

    if (!sessionId) {
        try {
            sessionId = await createChatSession({
                tenantId: context.tenantId,
                visitorId,
            });
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

    let tenantProjects = [];
    try {
        tenantProjects = await fetchProjectsForTenant(context.tenantId);
    } catch (err) {
        app.log.warn(
            { err: err.message || err },
            "Failed to load tenant projects for chat"
        );
    }

    const isRepoAllowed = (repo) => {
        if (!repo) {
            return false;
        }
        return tenantProjects.some((project) => {
            const parsed = parseRepoFromProject(project);
            return parsed && isSameRepo(parsed, repo);
        });
    };

    let historyRepo =
        history.length > 0 ? inferRepoFromHistory(history) : null;
    if (historyRepo && !isRepoAllowed(historyRepo)) {
        historyRepo = null;
    }

    const questionRepoMatch = await resolveRepoFromQuestion(
        question,
        tenantProjects
    );
    const questionRepo = questionRepoMatch?.repo || null;
    const questionRepoExplicit = Boolean(questionRepoMatch?.explicit);
    const repoFilterInput =
        body.repo || body.repoUrl || body.project || body.projectRepo;
    const repoInput = parseRepoFilter(repoFilterInput);
    let repoFilter =
        repoInput && isRepoAllowed(repoInput) ? repoInput : null;
    const hasContextRepo = Boolean(historyRepo || repoFilter);
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
                reply.raw.setHeader(
                    "Access-Control-Allow-Credentials",
                    "true"
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
                    tenantId: context.tenantId,
                    skipSemantic: statsQuestion,
                });
                const extras = [];
                if (statsQuestion) {
                    const statsRows = await fetchRepoStats(
                        10,
                        context.tenantId
                    );
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
                            6,
                            context.tenantId
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
                let usageTokens = null;
                const resolvedMaxTokens = Number.isFinite(chatMaxTokens)
                    ? chatMaxTokens
                    : 800;
                const streamOptions = {
                    model: chatModel,
                    stream: true,
                    stream_options: { include_usage: true },
                    messages: [
                        { role: "system", content: systemPrompt },
                        { role: "user", content: userPrompt },
                    ],
                };
                if (chatModelIsGpt5) {
                    streamOptions.max_completion_tokens = resolvedMaxTokens;
                } else {
                    streamOptions.max_tokens = resolvedMaxTokens;
                }
                if (chatModelSupportsTemperature) {
                    streamOptions.temperature = Number.isFinite(chatTemperature)
                        ? chatTemperature
                        : 0.2;
                }
                const stream = await openai.chat.completions.create(
                    streamOptions,
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
                    const usage = chunk.usage?.total_tokens;
                    if (Number.isFinite(usage)) {
                        usageTokens = usage;
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
                const usageEventId = await recordUsageEvent({
                    tenantId: context.tenantId,
                    sessionId,
                    tokens: usageTokens,
                    eventType: "chat_completion",
                });
                void reportUsageToStripe({
                    tenantId: context.tenantId,
                    usageEventId,
                    tokens: usageTokens,
                });
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
            tenantId: context.tenantId,
            skipSemantic: statsQuestion,
        });
        const extras = [];
        if (statsQuestion) {
            const statsRows = await fetchRepoStats(10, context.tenantId);
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
                    6,
                    context.tenantId
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

        const resolvedMaxTokens = Number.isFinite(chatMaxTokens)
            ? chatMaxTokens
            : 800;
        const completionOptions = {
            model: chatModel,
            messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: userPrompt },
            ],
        };
        if (chatModelIsGpt5) {
            completionOptions.max_completion_tokens = resolvedMaxTokens;
        } else {
            completionOptions.max_tokens = resolvedMaxTokens;
        }
        if (chatModelSupportsTemperature) {
            completionOptions.temperature = Number.isFinite(chatTemperature)
                ? chatTemperature
                : 0.2;
        }
        const completion = await openai.chat.completions.create(
            completionOptions
        );

        const answer = completion.choices?.[0]?.message?.content?.trim() || "";
        const usageTokens = completion.usage?.total_tokens;

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
        const usageEventId = await recordUsageEvent({
            tenantId: context.tenantId,
            sessionId,
            tokens: usageTokens,
            eventType: "chat_completion",
        });
        void reportUsageToStripe({
            tenantId: context.tenantId,
            usageEventId,
            tokens: usageTokens,
        });
    } catch (err) {
        reply.code(500).send({ error: err.message || "Chat failed" });
    }
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

const stripeUsageSyncIntervalMs = 60 * 60 * 1000;
if (billingEnabled && tokenMeterConfigured) {
    setInterval(() => {
        syncStripeUsageSnapshots("hourly");
    }, stripeUsageSyncIntervalMs);
    setTimeout(() => {
        syncStripeUsageSnapshots("startup");
    }, 5000);
}

const port = Number(process.env.API_PORT || process.env.PORT || 4011);
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
