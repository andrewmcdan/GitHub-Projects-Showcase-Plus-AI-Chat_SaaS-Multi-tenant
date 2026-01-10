ALTER TABLE "tenants" ADD COLUMN "handle" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "bio" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "is_public" boolean DEFAULT true NOT NULL;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "repo_limit" integer;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "token_limit" integer;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "tenants_handle_unique" ON "tenants" (lower("handle"));
