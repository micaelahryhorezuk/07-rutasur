FROM node:22.19.0-alpine3.22
WORKDIR /workspace
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY packages/contracts ./packages/contracts
COPY services/simulator ./services/simulator
COPY services/business ./services/business
COPY services/notifications ./services/notifications
COPY services/rescheduling ./services/rescheduling
COPY services/analytics ./services/analytics
ENV NODE_ENV=production
CMD ["node", "services/notifications/src/cli.js"]
