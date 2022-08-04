FROM node:alpine

RUN mkdir /src

WORKDIR /src

COPY ./infrastructures/front /src

RUN npm install

COPY ./src/services/front /src

ARG REACT_APP_URI_HOST

ENV REACT_APP_URI_HOST=$REACT_APP_URI_HOST

CMD ["npm", "start"]