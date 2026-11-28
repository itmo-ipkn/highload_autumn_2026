FROM node:24.11.1-bookworm-slim AS build
WORKDIR /app
RUN npm install -g pnpm@10.19.0
COPY package.json pnpm-lock.yaml ./
COPY pnpm-workspace.yaml ./
COPY patches ./patches
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

FROM build AS integration
COPY test ./test
COPY postgres ./postgres
RUN pnpm build
CMD ["sh", "-c", "node --test dist/test/integration/*.test.js"]

FROM build AS production-dependencies
RUN pnpm prune --prod

FROM node:24.11.1-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=production-dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER node
CMD ["node", "dist/src/producer/main.js"]
