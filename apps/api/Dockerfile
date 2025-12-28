FROM node:20-bullseye-slim

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
COPY apps/worker/package.json apps/worker/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/shared/package.json packages/shared/package.json

RUN npm ci

COPY apps/api apps/api
COPY packages/shared packages/shared
COPY projects.yaml projects.yaml

WORKDIR /app/apps/api

EXPOSE 3001

CMD ["npm", "run", "start"]
