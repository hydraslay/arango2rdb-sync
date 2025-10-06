package com.example.arango2rdb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.example.arango2rdb.config.SyncConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SyncService implements AutoCloseable {
    private final SyncConfig config;
    private final ArangoDB arangoDB;
    private final ArangoDatabase arangoDatabase;
    private final Connection connection;
    private final DatabaseMetaData databaseMetaData;
    private final Map<String, Map<String, Integer>> columnTypeCache = new HashMap<>();
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final List<SyncConfig.CollectionMapping> orderedMappings;

    public SyncService(SyncConfig config) throws SQLException {
        this.config = Objects.requireNonNull(config, "config");
        SyncConfig.ArangoConfig arango = config.arango;
        ArangoDB.Builder builder = new ArangoDB.Builder()
                .host(arango.host, arango.port)
                .user(arango.user)
                .password(arango.password);
        if (arango.useSsl) {
            builder.useSsl(true);
        }
        this.arangoDB = builder.build();
        this.arangoDatabase = arangoDB.db(arango.database);

        SyncConfig.RdbConfig rdb = config.rdb;
        this.connection = DriverManager.getConnection(rdb.jdbcUrl, rdb.user, rdb.password);
        this.connection.setAutoCommit(false);
        this.databaseMetaData = connection.getMetaData();
        this.orderedMappings = orderMappings(config.collections);
    }

    public void run() throws SQLException {
        for (SyncConfig.CollectionMapping mapping : orderedMappings) {
            syncCollection(mapping);
        }
    }

    private List<SyncConfig.CollectionMapping> orderMappings(List<SyncConfig.CollectionMapping> mappings) {
        Map<String, SyncConfig.CollectionMapping> byTable = new HashMap<>();
        List<String> originalOrder = new ArrayList<>();
        for (SyncConfig.CollectionMapping mapping : mappings) {
            String tableKey = normalizeTable(mapping.table);
            byTable.put(tableKey, mapping);
            originalOrder.add(tableKey);
        }

        Map<String, Integer> dependencyCounts = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        for (SyncConfig.CollectionMapping mapping : mappings) {
            String tableKey = normalizeTable(mapping.table);
            Set<String> uniqueDependencies = new HashSet<>();
            for (String dependency : mapping.dependsOn) {
                String depKey = normalizeTable(dependency);
                if (uniqueDependencies.add(depKey)) {
                    dependents.computeIfAbsent(depKey, k -> new ArrayList<>()).add(tableKey);
                }
            }
            dependencyCounts.put(tableKey, uniqueDependencies.size());
        }

        Deque<String> ready = new ArrayDeque<>();
        Set<String> enqueued = new HashSet<>();
        for (String tableKey : originalOrder) {
            if (dependencyCounts.getOrDefault(tableKey, 0) == 0) {
                ready.addLast(tableKey);
                enqueued.add(tableKey);
            }
        }

        List<SyncConfig.CollectionMapping> ordered = new ArrayList<>(mappings.size());
        while (!ready.isEmpty()) {
            String tableKey = ready.removeFirst();
            ordered.add(byTable.get(tableKey));
            for (String dependent : dependents.getOrDefault(tableKey, List.of())) {
                int remaining = dependencyCounts.get(dependent) - 1;
                dependencyCounts.put(dependent, remaining);
                if (remaining == 0 && enqueued.add(dependent)) {
                    ready.addLast(dependent);
                }
            }
        }

        if (ordered.size() != mappings.size()) {
            throw new IllegalArgumentException("Detected circular dependency between collection mappings");
        }
        return ordered;
    }

    private String normalizeTable(String table) {
        return table.toLowerCase(Locale.ROOT);
    }

    private void syncCollection(SyncConfig.CollectionMapping mapping) throws SQLException {
        System.out.printf(Locale.US, "Syncing collection %s -> table %s%n", mapping.collection, mapping.table);
        Map<String, Object> bindVars = Map.of("@collection", mapping.collection);
        try (ArangoCursor<BaseDocument> cursor = arangoDatabase.query(
                "FOR doc IN @@collection RETURN doc",
                bindVars,
                null,
                BaseDocument.class)) {
            while (cursor.hasNext()) {
                BaseDocument document = cursor.next();
                upsertDocument(mapping, document);
            }
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw new SQLException("Failed to sync collection " + mapping.collection, ex);
        }
    }

    private void upsertDocument(SyncConfig.CollectionMapping mapping, BaseDocument document) throws SQLException {
        Object keyValue = resolveValue(document, mapping.keyField);
        if (keyValue == null) {
            keyValue = document.getKey();
        }
        if (keyValue == null) {
            throw new SQLException("Document in collection " + mapping.collection + " missing key field " + mapping.keyField);
        }

        Object sqlKeyValue = toSqlValue(mapping.table, mapping.keyColumn, keyValue);

        Map<String, String> mappings = new LinkedHashMap<>(mapping.fieldMappings);
        List<Map.Entry<String, String>> entries = new ArrayList<>(mappings.entrySet());
        entries.sort(Comparator.comparing(Map.Entry::getValue));

        List<String> updateColumns = new ArrayList<>();
        List<Object> updateValues = new ArrayList<>();
        for (Map.Entry<String, String> entry : entries) {
            if (entry.getValue().equals(mapping.keyColumn)) {
                continue;
            }
            updateColumns.add(entry.getValue());
            Object rawValue = resolveValue(document, entry.getKey());
            updateValues.add(toSqlValue(mapping.table, entry.getValue(), rawValue));
        }

        if (updateColumns.isEmpty()) {
            if (!exists(mapping.table, mapping.keyColumn, keyValue)) {
                insertRecord(mapping, sqlKeyValue, entries, document);
            }
            return;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(mapping.table).append(" SET ");
        for (int i = 0; i < updateColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(updateColumns.get(i)).append(" = ?");
        }
        sql.append(" WHERE ").append(mapping.keyColumn).append(" = ?");

        try (PreparedStatement update = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Object value : updateValues) {
                update.setObject(index++, value);
            }
            update.setObject(index, sqlKeyValue);
            int affected = update.executeUpdate();
            if (affected == 0) {
                insertRecord(mapping, sqlKeyValue, entries, document);
            }
        }
    }

    private void insertRecord(SyncConfig.CollectionMapping mapping,
                              Object sqlKeyValue,
                              List<Map.Entry<String, String>> entries,
                              BaseDocument document) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        columns.add(mapping.keyColumn);
        values.add(sqlKeyValue);

        for (Map.Entry<String, String> entry : entries) {
            if (entry.getValue().equals(mapping.keyColumn)) {
                continue;
            }
            columns.add(entry.getValue());
            Object rawValue = resolveValue(document, entry.getKey());
            values.add(toSqlValue(mapping.table, entry.getValue(), rawValue));
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(mapping.table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(columns.get(i));
        }
        sql.append(") VALUES (");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");

        try (PreparedStatement insert = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.size(); i++) {
                insert.setObject(i + 1, values.get(i));
            }
            insert.executeUpdate();
        }
    }

    private boolean exists(String table, String keyColumn, Object rawKeyValue) throws SQLException {
        String sql = "SELECT 1 FROM " + table + " WHERE " + keyColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, toSqlValue(table, keyColumn, rawKeyValue));
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    private Object resolveValue(BaseDocument document, String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            return null;
        }
        switch (fieldPath) {
            case "_key":
                return document.getKey();
            case "_id":
                return document.getId();
            case "_rev":
                return document.getRevision();
            default:
                return resolveFromMap(document.getProperties(), fieldPath);
        }
    }

    private Object resolveFromMap(Map<String, Object> map, String path) {
        if (map == null || path == null) {
            return null;
        }
        String[] parts = path.split("\\.");
        Object current = map;
        for (String part : parts) {
            if (!(current instanceof Map)) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> currentMap = (Map<String, Object>) current;
            current = currentMap.get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private Object toSqlValue(String table, String column, Object rawValue) throws SQLException {
        Object normalized = normalizeValue(rawValue);
        if (normalized == null) {
            return null;
        }
        int sqlType = resolveColumnType(table, column);
        return coerceToSqlType(normalized, sqlType);
    }

    private Object normalizeValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Instant) {
            return Timestamp.from((Instant) value);
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        if (value instanceof ZonedDateTime) {
            return Timestamp.from(((ZonedDateTime) value).toInstant());
        }
        if (value instanceof Map || value instanceof Iterable || value.getClass().isArray()) {
            try {
                return jsonMapper.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new SQLException("Failed to serialise complex type to JSON", e);
            }
        }
        return value.toString();
    }

    private int resolveColumnType(String table, String column) throws SQLException {
        String tableKey = normalizeTable(table);
        Map<String, Integer> types = columnTypeCache.get(tableKey);
        if (types == null) {
            types = loadColumnTypes(table);
            columnTypeCache.put(tableKey, types);
        }
        Integer sqlType = types.get(column.toLowerCase(Locale.ROOT));
        return sqlType != null ? sqlType : Types.OTHER;
    }

    private Map<String, Integer> loadColumnTypes(String table) throws SQLException {
        Map<String, Integer> types = new HashMap<>();
        String[] tablePatterns = new String[]{table, table.toUpperCase(Locale.ROOT), table.toLowerCase(Locale.ROOT)};
        for (String pattern : tablePatterns) {
            if (!types.isEmpty()) {
                break;
            }
            try (ResultSet columns = databaseMetaData.getColumns(connection.getCatalog(), null, pattern, "%")) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    int dataType = columns.getInt("DATA_TYPE");
                    types.put(columnName.toLowerCase(Locale.ROOT), dataType);
                }
            }
        }
        return types;
    }

    private Object coerceToSqlType(Object value, int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.DATE:
                return coerceToDate(value);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return coerceToTimestamp(value);
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return coerceToTime(value);
            default:
                return value;
        }
    }

    private Date coerceToDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof Timestamp) {
            return new Date(((Timestamp) value).getTime());
        }
        if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return Date.valueOf(((LocalDateTime) value).toLocalDate());
        }
        if (value instanceof OffsetDateTime) {
            return Date.valueOf(((OffsetDateTime) value).toLocalDate());
        }
        if (value instanceof ZonedDateTime) {
            return Date.valueOf(((ZonedDateTime) value).toLocalDate());
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Date.valueOf(LocalDate.parse(text));
            } catch (DateTimeParseException ex) {
                throw new SQLException("Failed to convert value '" + text + "' to DATE", ex);
            }
        }
        if (value instanceof Instant) {
            return new Date(((Instant) value).toEpochMilli());
        }
        throw new SQLException("Unsupported value type for DATE column: " + value.getClass().getName());
    }

    private Timestamp coerceToTimestamp(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof Instant) {
            return Timestamp.from((Instant) value);
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return Timestamp.valueOf(((LocalDate) value).atStartOfDay());
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        if (value instanceof ZonedDateTime) {
            return Timestamp.from(((ZonedDateTime) value).toInstant());
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Timestamp.valueOf(LocalDateTime.parse(text));
            } catch (DateTimeParseException ignored) {
                // Try instant parsing
            }
            try {
                return Timestamp.from(Instant.parse(text));
            } catch (DateTimeParseException ignored) {
                // Try date-only at start of day
            }
            try {
                return Timestamp.valueOf(LocalDate.parse(text).atStartOfDay());
            } catch (DateTimeParseException ex) {
                throw new SQLException("Failed to convert value '" + text + "' to TIMESTAMP", ex);
            }
        }
        throw new SQLException("Unsupported value type for TIMESTAMP column: " + value.getClass().getName());
    }

    private Time coerceToTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Time) {
            return (Time) value;
        }
        if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalTime) {
            return Time.valueOf((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return Time.valueOf(((LocalDateTime) value).toLocalTime());
        }
        if (value instanceof OffsetDateTime) {
            return Time.valueOf(((OffsetDateTime) value).toLocalTime());
        }
        if (value instanceof ZonedDateTime) {
            return Time.valueOf(((ZonedDateTime) value).toLocalTime());
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return Time.valueOf(LocalTime.parse(text));
            } catch (DateTimeParseException ex) {
                throw new SQLException("Failed to convert value '" + text + "' to TIME", ex);
            }
        }
        throw new SQLException("Unsupported value type for TIME column: " + value.getClass().getName());
    }

    @Override
    public void close() throws SQLException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } finally {
            if (arangoDB != null) {
                arangoDB.shutdown();
            }
        }
    }
}
