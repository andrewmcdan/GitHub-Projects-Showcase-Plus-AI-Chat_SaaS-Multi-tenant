import {
  boolean,
  integer,
  jsonb,
  numeric,
  pgTable,
  serial,
  text,
  timestamp,
  vector
} from "drizzle-orm/pg-core";

export const tenants = pgTable("tenants", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").references(() => tenants.id),
  email: text("email").notNull(),
  name: text("name"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const projects = pgTable("projects", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").references(() => tenants.id),
  name: text("name").notNull(),
  repoUrl: text("repo_url").notNull(),
  description: text("description"),
  tags: jsonb("tags"),
  featured: boolean("featured").default(false).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const sources = pgTable("sources", {
  id: serial("id").primaryKey(),
  projectId: integer("project_id").references(() => projects.id),
  repoOwner: text("repo_owner"),
  repoName: text("repo_name"),
  refType: text("ref_type"),
  ref: text("ref"),
  path: text("path"),
  commitSha: text("commit_sha"),
  url: text("url"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const chunks = pgTable("chunks", {
  id: serial("id").primaryKey(),
  sourceId: integer("source_id").references(() => sources.id),
  content: text("content").notNull(),
  embedding: vector("embedding", { dimensions: 1536 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const ingestJobs = pgTable("ingest_jobs", {
  id: serial("id").primaryKey(),
  projectId: integer("project_id").references(() => projects.id),
  projectRepo: text("project_repo").notNull(),
  projectName: text("project_name"),
  totalFiles: integer("total_files"),
  totalBytes: integer("total_bytes"),
  filesProcessed: integer("files_processed").default(0).notNull(),
  chunksStored: integer("chunks_stored").default(0).notNull(),
  status: text("status").notNull(),
  error: text("error"),
  lastMessage: text("last_message"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull(),
  startedAt: timestamp("started_at", { withTimezone: true }),
  finishedAt: timestamp("finished_at", { withTimezone: true }),
  updatedAt: timestamp("updated_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const chatSessions = pgTable("chat_sessions", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").references(() => tenants.id),
  visitorId: text("visitor_id"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const chatMessages = pgTable("chat_messages", {
  id: serial("id").primaryKey(),
  sessionId: integer("session_id").references(() => chatSessions.id),
  role: text("role").notNull(),
  content: text("content").notNull(),
  citations: jsonb("citations"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const usageEvents = pgTable("usage_events", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").references(() => tenants.id),
  sessionId: integer("session_id").references(() => chatSessions.id),
  eventType: text("event_type").notNull(),
  tokens: integer("tokens"),
  costUsd: numeric("cost_usd"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});

export const apiKeys = pgTable("api_keys", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").references(() => tenants.id),
  name: text("name"),
  keyHash: text("key_hash").notNull(),
  lastUsedAt: timestamp("last_used_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true })
    .defaultNow()
    .notNull()
});
