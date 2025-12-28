# GitHub Projects Homepage + AI Chat

## What this MVP does
- Curated project list from `projects.yaml`
- Public chat widget that answers using indexed GitHub docs
- Citations back to GitHub files/refs

## Local development (scaffold)
1) Copy `.env.example` to `.env` and fill in values.
2) Install deps at repo root: `npm install`
3) Start apps:
   - API: `npm --workspace apps/api run dev`
   - Worker: `npm --workspace apps/worker run dev`
   - Web: `npm --workspace apps/web run dev`

## Deployment notes
- Target: single Lightsail instance with Docker Compose.
- DNS: Lightsail DNS.
- Full checklist: `MVP Build Checklist.md`.

## Docker Compose (local)
1) Ensure `.env` exists.
2) Run: `docker compose -f infra/compose/docker-compose.yml up --build`
3) Stop: `docker compose -f infra/compose/docker-compose.yml down`

## Tech choices
- Backend: Node + Fastify
- Frontend: Next.js
- DB/Vector: Postgres + pgvector
- Object store: MinIO
- LLM: OpenAI API
- GitHub auth: GitHub App
