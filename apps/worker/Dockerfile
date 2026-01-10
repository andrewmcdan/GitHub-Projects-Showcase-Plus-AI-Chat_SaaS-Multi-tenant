FROM node:20-bullseye-slim

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
COPY apps/worker/package.json apps/worker/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/shared/package.json packages/shared/package.json

RUN npm ci

COPY apps/worker apps/worker
COPY packages/shared packages/shared

WORKDIR /app/apps/worker

CMD ["npm", "run", "start"]
