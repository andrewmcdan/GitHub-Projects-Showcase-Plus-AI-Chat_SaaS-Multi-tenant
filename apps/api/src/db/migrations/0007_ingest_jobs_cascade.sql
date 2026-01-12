ALTER TABLE "ingest_jobs" DROP CONSTRAINT "ingest_jobs_project_id_projects_id_fk";
ALTER TABLE "ingest_jobs" ADD CONSTRAINT "ingest_jobs_project_id_projects_id_fk" FOREIGN KEY ("project_id") REFERENCES "public"."projects"("id") ON DELETE cascade ON UPDATE no action;
