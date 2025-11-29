FROM node:20-alpine AS builder

WORKDIR /app

COPY example_app/package*.json ./

RUN npm ci --only=production

COPY js_api/ js_api/
COPY example_app/ ./

FROM node:20-alpine

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/js_api ./js_api

COPY example_app/ ./

EXPOSE 8080

CMD ["node", "src/app.js"]
