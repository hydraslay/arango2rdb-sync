# ArangoDB to Relational Sync Sample

This workspace contains a Java CLI application plus container assets that demonstrate syncing data from ArangoDB into a relational database (PostgreSQL by default). The sample graph domain models project delivery across teams, with six ArangoDB collections and corresponding relational tables.

## Repository layout

- `app/` - Maven project for the sync CLI and sample `config/mapping.json`.
- `arangodb-docker/` - Docker build context that provisions ArangoDB with sample data (six related collections).
- `postgres-docker/` - Docker build context that provisions PostgreSQL with schema ready for sync.
- `docker-compose.yml` - Convenience stack to launch both databases locally.

## Java CLI

Build (requires Maven and network access the first time):

```bash
cd app
mvn -DskipTests package
```

The shaded JAR will be produced at `app/target/arango2rdb-sync-1.0.0-SNAPSHOT-shaded.jar`.

## or directly Run with source
```
mvn compile exec:java -Dexec.mainClass="com.example.arango2rdb.App"
```

### Commands

```bash
java -jar target/arango2rdb-sync-1.0.0-SNAPSHOT-shaded.jar <command> [configPath]
```

- `sync` (default) - run the sync according to the mapping JSON.
- `describe-arango` - list Arango collections plus one sample document each.
- `describe-rdb` - list relational tables and column metadata.
- `help` - display usage.

If `configPath` is omitted the CLI loads `config/mapping.json` relative to the working directory.

### Mapping JSON

`config/mapping.json` documents how ArangoDB collections map to relational tables. Each entry provides:

- `collection` - source collection name.
- `table` - target table name.
- `keyField` / `keyColumn` - document field and SQL column that serve as the upsert key.
- `fieldMappings` - property-to-column mapping (key column may be repeated for clarity).

Complex/nested values are serialised to JSON strings automatically when syncing.

## Containers

Launch both databases (requires Docker):

```bash
docker compose up --build
```

### ArangoDB image

- User: `root`
- Password: `arango2rdb`
- Database: `project_graph`
- Sample collections: `teams`, `members`, `projects`, `tasks`, `task_assignments`, `milestones`

The init script refreshes data on container start for deterministic demos.

### PostgreSQL image

- Database: `project_sync`
- User: `postgres`
- Password: `postgres`
- Schema mirrors the sample collections with sensible foreign keys and data types.

## Running a full sync

1. Start the containers: `docker compose up --build`.
2. In another terminal, run `java -jar target/arango2rdb-sync-1.0.0-SNAPSHOT-shaded.jar describe-arango` (optional) to inspect collections.
3. Run `java -jar target/arango2rdb-sync-1.0.0-SNAPSHOT-shaded.jar sync` to populate PostgreSQL.
4. Inspect the relational data, e.g. `docker exec -it <postgres-container> psql -U postgres -d project_sync -c "SELECT * FROM projects;"`.

## Adapting to other RDBMS

- Update the `rdb` block in the mapping file with the target JDBC URL, username, and password.
- Ensure the destination tables exist (the tool assumes schema already created).
- For vendors without transactional DDL, adjust the schema SQL accordingly.

## Notes

- The CLI opens a transaction per collection; on failure it rolls back only the affected collection.
- Arrays or embedded documents are serialised as JSON strings when sent to SQL columns.
- For other authentication schemes (e.g., ArangoDB JWT, PostgreSQL SSL), extend the configuration classes and mapping file.
