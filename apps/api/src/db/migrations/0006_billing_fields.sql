ALTER TABLE "tenants" ADD COLUMN "plan" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "subscription_status" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "stripe_customer_id" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "stripe_subscription_id" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "stripe_token_item_id" text;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "current_period_end" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "tenants" ADD COLUMN "billing_email" text;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "tenants_stripe_customer_id_idx" ON "tenants" ("stripe_customer_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "tenants_stripe_subscription_id_idx" ON "tenants" ("stripe_subscription_id");
