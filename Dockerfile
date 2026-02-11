FROM node:20-alpine AS builder

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

FROM node:20-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --omit=dev

COPY --from=builder /usr/src/app/dist ./dist
# The below would fail if the seeder script is written in TS and not compiled.
# I will compile everything to dist, including the seeder script if possible.
# But for simplicity, I'll just use the `npm run start` which runs the built JS.

EXPOSE 8080

CMD ["npm", "run", "start"]
