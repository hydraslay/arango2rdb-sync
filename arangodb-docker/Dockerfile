FROM arangodb:3.11.7

ENV ARANGO_ROOT_PASSWORD=openSesame
ENV ARANGO_NO_AUTH=0

COPY initdb.d/ /docker-entrypoint-initdb.d/

EXPOSE 8529
