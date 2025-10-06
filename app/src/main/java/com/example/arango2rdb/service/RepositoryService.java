package com.example.arango2rdb.service;

import com.example.arango2rdb.config.SyncConfig;
import com.example.arango2rdb.view.RepositoryInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Service
public class RepositoryService {

    private static final String METADATA_TABLE = "sync_repositories";
    private static final String STATUS_READY = "READY";
    private static final String STATUS_COMMITTED = "COMMITTED";

    private final JdbcTemplate jdbcTemplate;
    private final SyncConfig config;

    public RepositoryService(JdbcTemplate jdbcTemplate, SyncConfig config) {
        this.jdbcTemplate = jdbcTemplate;
        this.config = config;
    }

    @PostConstruct
    void ensureMetadataTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + METADATA_TABLE + " (" +
                "id BIGSERIAL PRIMARY KEY, " +
                "name VARCHAR(255) NOT NULL UNIQUE, " +
                "schema_name VARCHAR(255) NOT NULL UNIQUE, " +
                "status VARCHAR(50) NOT NULL DEFAULT '" + STATUS_READY + "', " +
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                ")");
    }

    public List<RepositoryInfo> listRepositories() {
        return jdbcTemplate.query(
                "SELECT id, name, schema_name, status, created_at FROM " + METADATA_TABLE + " ORDER BY created_at DESC",
                repositoryRowMapper());
    }

    public Optional<RepositoryInfo> findById(long id) {
        List<RepositoryInfo> rows = jdbcTemplate.query(
                "SELECT id, name, schema_name, status, created_at FROM " + METADATA_TABLE + " WHERE id = ?",
                repositoryRowMapper(),
                id);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public RepositoryInfo createRepository(String requestedName) {
        String name = Optional.ofNullable(requestedName)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("Repository name must not be blank"));

        String slug = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
        if (slug.isBlank()) {
            throw new IllegalArgumentException("Repository name must contain alphanumeric characters");
        }
        String schemaName = "repo_" + slug;

        if (existsByName(name)) {
            throw new IllegalArgumentException("Repository name already exists: " + name);
        }
        if (existsBySchema(schemaName)) {
            throw new IllegalArgumentException("Repository schema already exists: " + schemaName);
        }

        createSchemaWithTables(schemaName);

        return jdbcTemplate.queryForObject(
                "INSERT INTO " + METADATA_TABLE + " (name, schema_name, status, created_at) " +
                        "VALUES (?, ?, ?, CURRENT_TIMESTAMP) RETURNING id, name, schema_name, status, created_at",
                repositoryRowMapper(),
                name,
                schemaName,
                STATUS_READY);
    }

    public void deleteRepository(long id) {
        RepositoryInfo info = findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Repository not found: " + id));
        jdbcTemplate.update("DELETE FROM " + METADATA_TABLE + " WHERE id = ?", id);
        jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + renderIdentifier(info.schemaName()) + " CASCADE");
    }

    public void markCommitted(long id) {
        jdbcTemplate.update("UPDATE " + METADATA_TABLE + " SET status = ? WHERE id = ?", STATUS_COMMITTED, id);
    }

    private boolean existsByName(String name) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + METADATA_TABLE + " WHERE name = ?",
                Integer.class,
                name);
        return count != null && count > 0;
    }

    private boolean existsBySchema(String schemaName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + METADATA_TABLE + " WHERE schema_name = ?",
                Integer.class,
                schemaName);
        return count != null && count > 0;
    }

    private void createSchemaWithTables(String schemaName) {
        jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS " + renderIdentifier(schemaName));
        for (SyncConfig.MergeMapping merge : config.merges) {
            String qualifiedTarget = renderIdentifier(schemaName) + "." + renderIdentifier(merge.targetTable);
            String baseTable = renderIdentifier(merge.targetTable);
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + qualifiedTarget +
                    " (LIKE " + baseTable + " INCLUDING ALL)");
            jdbcTemplate.execute("TRUNCATE TABLE " + qualifiedTarget);
        }
    }

    private RowMapper<RepositoryInfo> repositoryRowMapper() {
        return (ResultSet rs, int rowNum) -> new RepositoryInfo(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("schema_name"),
                rs.getString("status"),
                toInstant(rs, "created_at"));
    }

    private Instant toInstant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private String renderIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
