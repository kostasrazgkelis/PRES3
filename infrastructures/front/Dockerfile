FROM node:alpine

RUN mkdir /src

WORKDIR /src

COPY ./infrastructures/front /src

RUN npm install

COPY ./src/services/front /src

# add arguments #
ARG REACT_APP_URI_HOST
ARG REACT_APP_URI_HOST
ARG REACT_APP_NAME_OF_CLUSTER

ENV REACT_APP_URI_HOST=$REACT_APP_URI_HOST
ENV REACT_APP_HDFS_HOST=$REACT_APP_HDFS_HOST
ENV REACT_APP_NAME_OF_CLUSTER=$REACT_APP_NAME_OF_CLUSTER
# stop argumetns #

CMD ["npm", "start"]