# GitHub Projects Homepage + AI Chat

A curated homepage for your GitHub portfolio with an AI assistant that answers
questions using indexed repository content and cites the exact files.

## Overview
This app turns a short list of repos into a searchable, citeable knowledge base.
It ingests repo code and docs, stores embeddings in Postgres + pgvector, and
streams chat responses with source links.

## Core capabilities
- Project catalog sourced from `projects.yaml` (managed via the UI in admin mode).
- Full-repo ingest (code + docs) with chunking and embeddings.
- Hybrid retrieval (vector + lexical) with citations per answer.
- SSE streaming chat with session history per browser.
- Admin controls: add/remove repos, reindex per project, view ingest jobs,
  retry/cancel jobs.
- Configurable session expiry (default 90 days).

## How it works
1. Projects are listed in `projects.yaml` (or added via the UI).
2. The worker ingests each repo, stores files in MinIO, and writes chunks +
   embeddings to Postgres.
3. The API retrieves relevant chunks, builds context, and streams responses with
   citations.
4. The web UI displays the catalog, chat, sources, and job status.

## Architecture
- Web: Next.js UI
- API: Fastify + OpenAI
- Worker: BullMQ ingest pipeline
- Storage: Postgres + pgvector, MinIO, Redis

## Admin mode
Admin actions (add/remove repos, reindex, delete chats, job controls) require
`ADMIN_API_KEY` and the `x-admin-key` header. In the UI, click the tiny Admin
button in the bottom-left corner to enter the key; right-click items to see admin
actions.

## Quick start (Docker)
1. Copy `.env.example` to `.env` and fill in credentials.
2. Run: `docker compose -f infra/compose/docker-compose.yml up --build`
3. Open `http://localhost:3000`

## Notes
- Local sessions are per browser (based on a stored visitor id).
- Use a GitHub App or PAT to avoid rate limits during ingest.
