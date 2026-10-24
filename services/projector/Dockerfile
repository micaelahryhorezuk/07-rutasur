FROM node:22.19.0-alpine3.22

WORKDIR /workspace
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY packages/contracts ./packages/contracts
COPY services/simulator ./services/simulator
COPY services/projector ./services/projector

ENV NODE_ENV=production
ENTRYPOINT ["node", "services/projector/src/cli.js"]
CMD ["run"]