FROM node:24.21.0-bookworm-slim AS build
WORKDIR /app
RUN npm install --global pnpm@10.19.0
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
COPY tests ./tests
RUN pnpm run build && pnpm prune --prod

FROM node:24.21.0-bookworm-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /app/package.json ./
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist/src ./dist/src
USER node
EXPOSE 3000
CMD ["node", "dist/src/server.js"]
