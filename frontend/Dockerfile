FROM node:18-alpine

WORKDIR /app

# Копирование package.json и package-lock.json
COPY package*.json ./

# Установка зависимостей
RUN npm install

# Копирование остального кода
COPY . .

# Expose port
EXPOSE 3000

# Команда запуска
CMD ["npm", "start"]

