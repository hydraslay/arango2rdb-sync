FROM postgres:16-alpine

ENV POSTGRES_DB=project_sync
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

COPY initdb.d/ /docker-entrypoint-initdb.d/

EXPOSE 5432
