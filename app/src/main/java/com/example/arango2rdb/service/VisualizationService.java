package com.example.arango2rdb.service;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.example.arango2rdb.config.SyncConfig;
import com.example.arango2rdb.view.CollectionSnapshot;
import com.example.arango2rdb.view.MergeView;
import com.example.arango2rdb.view.TableSnapshot;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Service
public class VisualizationService {

    private static final Logger log = LoggerFactory.getLogger(VisualizationService.class);

    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int PAGE_STEP = 10;

    private final ArangoDatabase arangoDatabase;
    private final JdbcTemplate jdbcTemplate;
    private final SyncConfig syncConfig;

    public VisualizationService(ArangoDatabase arangoDatabase, JdbcTemplate jdbcTemplate, SyncConfig syncConfig) {
        this.arangoDatabase = arangoDatabase;
        this.jdbcTemplate = jdbcTemplate;
        this.syncConfig = syncConfig;
    }

    public List<CollectionSnapshot> loadArangoCollections(String filter) {
        return arangoDatabase.getCollections().stream()
                .map(entity -> entity.getName())
                .filter(name -> !name.startsWith("_"))
                .sorted(String::compareToIgnoreCase)
                .filter(name -> filter == null || name.toLowerCase(Locale.ROOT).contains(filter.toLowerCase(Locale.ROOT)))
                .map(name -> tryLoadCollection(name, DEFAULT_PAGE_SIZE))
                .flatMap(Optional::stream)
                .toList();
    }

    private Optional<CollectionSnapshot> tryLoadCollection(String name, int size) {
        try {
            return Optional.of(loadArangoCollection(name, size));
        } catch (Exception ex) {
            log.warn("Failed to load collection {}", name, ex);
            return Optional.empty();
        }
    }

    public CollectionSnapshot loadArangoCollection(String name, int requestedSize) {
        long totalCount = arangoDatabase.collection(name).count().getCount();
        int limit = normalizeLimit(requestedSize, totalCount);
        List<Map<String, Object>> rows = new ArrayList<>();
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        columns.add("_key");
        columns.add("_id");
        columns.add("_rev");

        if (limit > 0) {
            String query = "FOR doc IN @@collection LIMIT @limit RETURN doc";
            Map<String, Object> bindVars = new HashMap<>();
            bindVars.put("@collection", name);
            bindVars.put("limit", limit);

            ArangoCursor<BaseDocument> cursor = arangoDatabase.query(query, bindVars, null, BaseDocument.class);
            try {
                while (cursor.hasNext()) {
                    BaseDocument doc = cursor.next();
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("_key", doc.getKey());
                    map.put("_id", doc.getId());
                    map.put("_rev", doc.getRevision());
                    doc.getProperties().forEach((k, v) -> {
                        columns.add(k);
                        map.put(k, simplifyValue(v));
                    });
                    rows.add(map);
                }
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to stream collection " + name, ex);
            } finally {
                try {
                    cursor.close();
                } catch (Exception closeEx) {
                    // ignore close failure
                }
            }
        }
        return new CollectionSnapshot(name, totalCount, limit, rows, new ArrayList<>(columns), PAGE_STEP);
    }

    public List<TableSnapshot> loadTables(String filter) {
        return readTableNames().stream()
                .filter(name -> filter == null || name.toLowerCase(Locale.ROOT).contains(filter.toLowerCase(Locale.ROOT)))
                .map(name -> loadTable(name, DEFAULT_PAGE_SIZE))
                .toList();
    }

    public TableSnapshot loadTable(String table, int requestedSize) {
        List<String> availableTables = readTableNames();
        if (!availableTables.contains(table)) {
            throw new IllegalArgumentException("Unknown table " + table);
        }
        long totalCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + quoteIdentifier(table), Long.class);
        int limit = normalizeLimit(requestedSize, totalCount);
        List<Map<String, Object>> rows = new ArrayList<>();
        List<String> columns = List.of();

        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + quoteIdentifier(table) + " LIMIT ?")) {
            statement.setInt(1, limit);
            try (ResultSet rs = statement.executeQuery()) {
                columns = resolveColumns(rs);
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (String column : columns) {
                        row.put(column, simplifyValue(rs.getObject(column)));
                    }
                    rows.add(row);
                }
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to read data from table " + table, ex);
        }

        if (columns.isEmpty()) {
            columns = resolveColumns(table);
        }

        return new TableSnapshot(table, totalCount, limit, rows, columns, PAGE_STEP);
    }

    public List<MergeView> getMergeMappings() {

        if (syncConfig.merges == null) {
            return List.of();
        }
        return syncConfig.merges.stream()
                .map(merge -> new MergeView(
                        merge.name,
                        merge.mainCollection,
                        merge.targetTable,
                        merge.fieldMappings.entrySet().stream()
                                .map(entry -> new MergeView.FieldMapping(entry.getKey(), entry.getValue()))
                                .toList(),
                        merge.joins.stream()
                                .map(join -> new MergeView.JoinMapping(
                                        join.alias,
                                        join.collection,
                                        join.localField,
                                        join.foreignField,
                                        join.required))
                                .toList()))
                .toList();
    }

    private Connection getConnection() throws SQLException {
        return Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
    }

    private int normalizeLimit(int requestedSize, long totalCount) {
        if (totalCount == 0) {
            return 0;
        }
        if (requestedSize <= 0 || requestedSize >= totalCount) {
            return (int) Math.min(totalCount, Integer.MAX_VALUE);
        }
        return requestedSize;
    }

    private Object simplifyValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        if (value instanceof Iterable<?> iterable) {
            return iterable;
        }
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        return value;
    }

    private List<String> readTableNames() {
        try (Connection connection = getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(connection.getCatalog(), null, "%", new String[]{"TABLE"})) {
                List<String> result = new ArrayList<>();
                while (tables.next()) {
                    String name = tables.getString("TABLE_NAME");
                    if (name != null && !name.startsWith("pg_")) {
                        result.add(name);
                    }
                }
                result.sort(String::compareToIgnoreCase);
                return result;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to read table metadata", ex);
        }
    }

    private List<String> resolveColumns(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        List<String> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(rs.getMetaData().getColumnLabel(i));
        }
        return columns;
    }

    private List<String> resolveColumns(String table) {
        try (Connection connection = getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(connection.getCatalog(), null, table, "%")) {
                List<String> result = new ArrayList<>();
                while (columns.next()) {
                    result.add(columns.getString("COLUMN_NAME"));
                }
                return result;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to fetch columns for table " + table, ex);
        }
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
