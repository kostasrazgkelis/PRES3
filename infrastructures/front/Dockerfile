FROM node:alpine

RUN mkdir /src

WORKDIR /src

COPY ./infrastructures/front /src

RUN npm install

COPY . /src

CMD ["npm", "start"]