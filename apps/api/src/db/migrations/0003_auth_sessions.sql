ALTER TABLE "users" ADD COLUMN "github_id" text;
ALTER TABLE "users" ADD COLUMN "github_username" text;
ALTER TABLE "users" ADD COLUMN "avatar_url" text;
--> statement-breakpoint
CREATE TABLE "auth_sessions" (
	"id" serial PRIMARY KEY NOT NULL,
	"user_id" integer,
	"tenant_id" integer,
	"token_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX "auth_sessions_token_hash_idx" ON "auth_sessions" ("token_hash");
--> statement-breakpoint
CREATE UNIQUE INDEX "users_github_id_idx" ON "users" ("github_id");
--> statement-breakpoint
ALTER TABLE "auth_sessions" ADD CONSTRAINT "auth_sessions_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "auth_sessions" ADD CONSTRAINT "auth_sessions_tenant_id_tenants_id_fk" FOREIGN KEY ("tenant_id") REFERENCES "public"."tenants"("id") ON DELETE no action ON UPDATE no action;
