ALTER TABLE "ingest_jobs" ADD COLUMN "total_files" integer;--> statement-breakpoint
ALTER TABLE "ingest_jobs" ADD COLUMN "total_bytes" integer;--> statement-breakpoint
ALTER TABLE "ingest_jobs" ADD COLUMN "files_processed" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "ingest_jobs" ADD COLUMN "chunks_stored" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "ingest_jobs" ADD COLUMN "last_message" text;--> statement-breakpoint
ALTER TABLE "ingest_jobs" ADD COLUMN "updated_at" timestamp with time zone DEFAULT now() NOT NULL;