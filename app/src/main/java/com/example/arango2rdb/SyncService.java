package com.example.arango2rdb;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.ArangoCursor;
import com.example.arango2rdb.config.SyncConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class SyncService implements AutoCloseable {
    private final SyncConfig config;
    private final ArangoDB arangoDB;
    private final ArangoDatabase arangoDatabase;
    private final Connection connection;
    private final ObjectMapper jsonMapper = new ObjectMapper();

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
    }

    public void run() throws SQLException {
        for (SyncConfig.CollectionMapping mapping : config.collections) {
            syncCollection(mapping);
        }
    }

    private void syncCollection(SyncConfig.CollectionMapping mapping) throws SQLException {
        System.out.printf(Locale.US, "Syncing collection %s -> table %s%n", mapping.collection, mapping.table);
        Map<String, Object> bindVars = Map.of("@collection", mapping.collection);
        try (ArangoCursor<BaseDocument> cursor = arangoDatabase.query("FOR doc IN @@collection RETURN doc", bindVars, null, BaseDocument.class)) {
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

        Map<String, String> mappings = new LinkedHashMap<>(mapping.fieldMappings);
        List<Map.Entry<String, String>> entries = new ArrayList<>(mappings.entrySet());
        entries.sort(Comparator.comparing(Map.Entry::getValue));

        List<String> updateColumns = new ArrayList<>();
        List<Object> updateValues = new ArrayList<>();
        for (Map.Entry<String, String> entry : entries) {
            if (entry.getValue().equals(mapping.keyColumn)) {
                // Avoid updating key column in the SET clause.
                continue;
            }
            updateColumns.add(entry.getValue());
            updateValues.add(normalizeValue(resolveValue(document, entry.getKey())));
        }

        Object normalizedKeyValue = normalizeValue(keyValue);

        if (updateColumns.isEmpty()) {
            // No columns to update; ensure record exists.
            if (!exists(mapping.table, mapping.keyColumn, normalizedKeyValue)) {
                insertRecord(mapping, normalizedKeyValue, entries, document);
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
            update.setObject(index, normalizedKeyValue);
            int affected = update.executeUpdate();
            if (affected == 0) {
                insertRecord(mapping, normalizedKeyValue, entries, document);
            }
        }
    }

    private void insertRecord(SyncConfig.CollectionMapping mapping,
                              Object normalizedKeyValue,
                              List<Map.Entry<String, String>> entries,
                              BaseDocument document) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        columns.add(mapping.keyColumn);
        values.add(normalizedKeyValue);

        for (Map.Entry<String, String> entry : entries) {
            if (entry.getValue().equals(mapping.keyColumn)) {
                continue;
            }
            columns.add(entry.getValue());
            values.add(normalizeValue(resolveValue(document, entry.getKey())));
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

    private boolean exists(String table, String keyColumn, Object keyValue) throws SQLException {
        String sql = "SELECT 1 FROM " + table + " WHERE " + keyColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, keyValue);
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
        if (value instanceof Map || value instanceof Iterable || value.getClass().isArray()) {
            try {
                return jsonMapper.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new SQLException("Failed to serialise complex type to JSON", e);
            }
        }
        return value.toString();
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
