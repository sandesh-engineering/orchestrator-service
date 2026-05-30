# BASE STAGE

FROM node:22-alpine AS base

WORKDIR /app

# Manages package managers like PNPM or Yarn
RUN corepack enable 

# BUILDER STAGE

FROM base AS builder

# Copy full monorepo (needed for workspace graph)
COPY . .

# Install all deps (workspace resolution happens here)
RUN pnpm install --frozen-lockfile

# Build only the service
RUN pnpm --filter workflow-orchestrator build

# Create production deploy output
RUN pnpm --filter workflow-orchestrator deploy --prod /prod/workflow-orchestrator

# RUNNER STAGE

FROM node:22-alpine AS runner

WORKDIR /app

ENV APP_ENV=production  

# Install dumb-init for proper signal handling (Proper process id 1 handling)
RUN apk add --no-cache dumb-init

# Create non-root group
RUN addgroup -g 1001 -S appgroup

# Create non-root user
RUN adduser -S -u 1001 -G appgroup appuser

# Copy only from deployed output
COPY --from=builder /prod/workflow-orchestrator ./

# As winston is setup to write in FS and no writes are enabled for the non-root user so we are enabling this
RUN mkdir -p /app/logs && chown -R 1001:1001 /app/logs

# Switch to non-root user
USER 1001:1001

EXPOSE 3000

# Graceful shutdown support
ENTRYPOINT ["dumb-init", "--"]

# We need to include the migrations script too.
CMD ["node", "dist/index.js"]
