FROM node:20-alpine AS builder

WORKDIR /app

COPY example_app/package*.json ./

RUN npm ci --only=production

COPY example_app/ ./

FROM node:20-alpine

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules

COPY example_app/ ./

EXPOSE 8080

CMD ["node", "src/app.js"]
