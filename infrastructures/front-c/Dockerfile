FROM node:alpine

RUN mkdir /src

WORKDIR /src

COPY ./infrastructures/front-c /src

RUN npm install

COPY ./src/services/front-c /src

ARG REACT_APP_URI_HOST

ENV REACT_APP_URI_HOST=$REACT_APP_URI_HOST

CMD ["npm", "start"]