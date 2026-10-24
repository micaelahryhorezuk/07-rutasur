FROM node:22.19.0-alpine3.22
WORKDIR /workspace
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY packages/contracts ./packages/contracts
COPY services/simulator ./services/simulator
COPY services/business ./services/business
COPY services/sla-monitor ./services/sla-monitor
ENV NODE_ENV=production
CMD ["node", "services/sla-monitor/src/cli.js"]
