# v2 Roadmap

## Vision
Make the showcase feel like a lightweight, shareable "project interview" portal:
faster onboarding, clearer value for recruiters, and better long-term retention
for builders.

## Guiding Principles
- Optimize for public visitor experience without hurting owner controls.
- Keep onboarding simple: connect GitHub App, add repos, share link.
- Maintain predictable cost and clear usage visibility.

## Milestones

### M1: Reliability + Billing Confidence (2-3 weeks)
Goals: reduce support friction and make billing state trustworthy.
- Confirm Stripe state: refresh subscription status on invoice events.
- Harden usage reporting: hourly "latest value" meter sync.
- Fix destructive flows: repo delete + ingest_jobs cascade in DB.
- Improve error surfacing: actionable errors for ingest and webhook failures.
- Add small admin checks: "Last indexed", "Reindex", ingest status badge.

Deliverables:
- Clear billing status and usage numbers.
- Stable repo deletion and ingest cleanup.

### M2: Public Experience + Discovery (3-4 weeks)
Goals: improve browsing and Q&A clarity for public visitors.
- Add category filter on public page.
- Add pinned projects and "Top repos" section.
- Add per-repo quick prompts (architecture, tradeoffs, etc.).
- Improve citation UX: collapsible sources + copy link.
- Add shareable chat sessions (public link to a Q&A).

Deliverables:
- Faster discovery and cleaner public showcase.

### M3: Owner Experience + Teams (4-6 weeks)
Goals: make ownership and collaboration smoother.
- Team roles (owner/editor/viewer) for orgs.
- Org-level GitHub App install status view.
- Org billing management (single plan, multiple contributors).
- Bulk repo actions: categorize, reindex, remove.

Deliverables:
- Team-ready product for org showcases.

### M4: Intelligence + Search (4-6 weeks)
Goals: stronger AI utility for repo interviews.
- Multi-repo chat context selection.
- Query-time repo routing (auto-select best repo).
- "Interview summaries" per repo (AI generated, editable).
- Semantic search across repo contents.

Deliverables:
- High-signal AI insights with faster retrieval.

## Cross-Cutting Workstreams
- Security: token scope validation, webhook validation alerts.
- Performance: cache repo metadata and snippet fetches.
- UX polish: consistent empty states and status messaging.

## Metrics to Track
- Onboarding completion rate (install app + add first repo).
- Public chat engagement rate (questions per visit).
- Retention (return visits from share links).
- Support tickets per 100 accounts.

## Open Questions
- Do we need a free tier with hard caps? Yes. Details TBD.
- Should public chat require a visible "owner active" badge? No.
- What is the minimal team feature set for v2 launch? TBD.
