FROM node:18-alpine

WORKDIR /app

# Копируем зависимости сначала, чтобы кэш слоёв работал лучше
COPY package*.json ./
RUN npm ci --only=production

COPY src ./src

EXPOSE 3001

CMD ["node", "src/index.js"] 
