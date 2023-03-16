FROM node:16.15.1-bullseye

WORKDIR /usr/src/app

COPY ./node-backend/package.json .
COPY ./node-backend/package-lock.json .

RUN npm install

COPY ./node-backend .

EXPOSE 3000

CMD [ "node", "index.js" ]