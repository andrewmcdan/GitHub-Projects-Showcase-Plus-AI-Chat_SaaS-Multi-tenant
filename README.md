# GitHub Projects Homepage + AI Chat

**Live demo:** https://a-mcd.com/showcase

A curated homepage for your GitHub portfolio with an AI assistant that answers
questions using indexed repository content and cites the exact files.

## Overview
This app turns a short list of repos into a searchable, citeable knowledge base.
It ingests repo code and docs, stores embeddings in Postgres + pgvector, and
streams chat responses with source links.

## Core capabilities
- Multi-tenant GitHub OAuth with per-user repo catalogs.
- Full-repo ingest (code + docs) with chunking and embeddings.
- Hybrid retrieval (vector + lexical) with citations per answer.
- SSE streaming chat with session history per browser.
- Public, shareable showcase URLs per user.
- Account dashboard for repo management, usage, and billing placeholders.
- Configurable session expiry (default 90 days).

## How it works
1. Projects are added from the account dashboard and stored per tenant.
2. The worker ingests each repo, stores files in MinIO, and writes chunks +
   embeddings to Postgres.
3. The API retrieves relevant chunks, builds context, and streams responses with
   citations.
4. The web UI displays the catalog, chat, and sources on a public URL.

## Architecture
- Web: Next.js UI
- API: Fastify + OpenAI
- Worker: BullMQ ingest pipeline
- Storage: Postgres + pgvector, MinIO, Redis

## Accounts
Sign in with GitHub to manage your repos, view usage, and copy your public
showcase URL.

## Quick start (Docker)
1. Copy `.env.example` to `.env` and fill in credentials.
2. Run: `docker compose -f infra/compose/docker-compose.yml up --build`
3. Open `http://localhost:3000`

## Notes
- Local sessions are per browser (based on a stored visitor id).
- Use a GitHub App or PAT to avoid rate limits during ingest.
