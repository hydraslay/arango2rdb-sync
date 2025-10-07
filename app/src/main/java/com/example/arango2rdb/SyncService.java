package com.example.arango2rdb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.ArangoDBException;
import com.example.arango2rdb.config.SyncConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.arangodb.model.CollectionCreateOptions;

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
import java.util.ArrayList;
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
    private final List<SyncConfig.MergeMapping> mergeMappings;

    private record TargetTable(String schema, String table) { }


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

        ArangoDatabase database;
        try {
            ensureDatabase(arango.database);
            database = arangoDB.db(arango.database);
        } catch (ArangoDBException ex) {
            throw new SQLException("Failed to initialise ArangoDB database " + arango.database, ex);
        }
        this.arangoDatabase = database;

        SyncConfig.RdbConfig rdb = config.rdb;
        this.connection = DriverManager.getConnection(rdb.jdbcUrl, rdb.user, rdb.password);
        this.connection.setAutoCommit(false);
        this.databaseMetaData = connection.getMetaData();
        this.mergeMappings = config.merges != null ? List.copyOf(config.merges) : List.of();
        ensureCollections(config);
    }

    private TargetTable targetTable(String repositorySchema, String table) {
        if (repositorySchema == null || repositorySchema.isBlank()) {
            return new TargetTable(null, table);
        }
        return new TargetTable(repositorySchema, table);
    }

    private String displayTable(TargetTable table) {
        if (table.schema() == null || table.schema().isBlank()) {
            return table.table();
        }
        return table.schema() + "." + table.table();
    }

    private String renderTable(TargetTable table) {
        if (table.schema() == null || table.schema().isBlank()) {
            return quoteIdentifier(table.table());
        }
        return quoteIdentifier(table.schema()) + "." + quoteIdentifier(table.table());
    }

    private String tableCacheKey(TargetTable table) {
        if (table.schema() == null || table.schema().isBlank()) {
            return table.table().toLowerCase(Locale.ROOT);
        }
        return (table.schema() + "." + table.table()).toLowerCase(Locale.ROOT);
    }

    private String schemaPattern(TargetTable table) throws SQLException {
        if (table.schema() == null || table.schema().isBlank()) {
            String defaultSchema = connection.getSchema();
            if (defaultSchema == null || defaultSchema.isBlank()) {
                return null;
            }
            return defaultSchema;
        }
        return table.schema();
    }

    private String renderColumn(String column) {
        return quoteIdentifier(column);
    }

    public void run() throws SQLException {
        run(null);
    }

    public void run(String repositorySchema) throws SQLException {
        for (SyncConfig.MergeMapping merge : mergeMappings) {
            syncMerge(merge, repositorySchema);
        }
    }

    private void syncMerge(SyncConfig.MergeMapping merge, String repositorySchema) throws SQLException {
        TargetTable targetTable = targetTable(repositorySchema, merge.targetTable);
        System.out.printf(Locale.US, "Syncing merge %s -> table %s%n", merge.name, displayTable(targetTable));
        Map<String, Object> bindVars = Map.of("@collection", merge.mainCollection);
        try (ArangoCursor<BaseDocument> cursor = arangoDatabase.query(
                "FOR doc IN @@collection RETURN doc",
                bindVars,
                null,
                BaseDocument.class)) {
            while (cursor.hasNext()) {
                BaseDocument mainDoc = cursor.next();
                Map<String, BaseDocument> context = new HashMap<>();
                context.put("main", mainDoc);

                boolean skip = false;
                for (SyncConfig.MergeJoin join : merge.joins) {
                    BaseDocument joinDoc;
                    if (join.connectedEdges != null && !join.connectedEdges.isEmpty()) {
                        joinDoc = fetchJoinDocumentViaEdges(context, join);
                    } else {
                        Object localValue = resolveAliasPath(context, join.localField);
                        joinDoc = null;
                        if (localValue != null) {
                            joinDoc = fetchJoinDocument(join, localValue);
                        }
                    }
                    if (joinDoc == null) {
                        if (join.required) {
                            skip = true;
                            break;
                        }
                        context.remove(join.alias);
                    } else {
                        context.put(join.alias, joinDoc);
                    }
                }
                if (skip) {
                    continue;
                }

                Object keyRaw = resolveAliasPath(context, merge.keyField);
                if (keyRaw == null) {
                    throw new SQLException("Merge '" + merge.name + "' missing key field " + merge.keyField
                            + " for main document " + mainDoc.getKey());
                }

                Map<String, Object> columnValues = new LinkedHashMap<>();
                for (Map.Entry<String, String> entry : merge.fieldMappings.entrySet()) {
                    Object value = resolveAliasPath(context, entry.getKey());
                    columnValues.put(entry.getValue(), value);
                }

                upsertRow(targetTable, merge.keyColumn, keyRaw, columnValues);
            }
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw new SQLException("Failed to sync merge " + merge.name, ex);
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

    private Object resolveAliasPath(Map<String, BaseDocument> context, String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String[] parts = path.split("\\.", 2);
        String alias = parts[0];
        BaseDocument document = context.get(alias);
        if (document == null) {
            return null;
        }
        if (parts.length == 1) {
            return document;
        }
        return resolveValue(document, parts[1]);
    }

    private BaseDocument fetchJoinDocument(SyncConfig.MergeJoin join, Object localValue) throws SQLException {
        Map<String, Object> bindVars = Map.of("value", localValue);
        String query = "FOR doc IN " + join.collection + " FILTER " + buildFieldAccess("doc", join.foreignField)
                + " == @value LIMIT 1 RETURN doc";
        try (ArangoCursor<BaseDocument> cursor = arangoDatabase.query(query, bindVars, null, BaseDocument.class)) {
            if (cursor.hasNext()) {
                return cursor.next();
            }
        } catch (Exception ex) {
            throw new SQLException("Failed to load join '" + join.alias + "' from collection " + join.collection, ex);
        }
        return null;
    }

    private BaseDocument fetchJoinDocumentViaEdges(Map<String, BaseDocument> context, SyncConfig.MergeJoin join) throws SQLException {
        BaseDocument start = context.get("main");
        if (start == null) {
            throw new SQLException("Missing main document when resolving connectedEdges for join '" + join.alias + "'");
        }
        String currentId = start.getId();
        if (currentId == null || currentId.isBlank()) {
            return null;
        }
        String nextId = currentId;
        for (SyncConfig.MergeJoin.ConnectedEdge edge : join.connectedEdges) {
            nextId = resolveEdgeStep(edge, nextId, join);
            if (nextId == null || nextId.isBlank()) {
                return null;
            }
        }
        return fetchDocumentById(join, nextId);
    }

    private String resolveEdgeStep(SyncConfig.MergeJoin.ConnectedEdge edge, String currentId, SyncConfig.MergeJoin join) throws SQLException {
        SyncConfig.MergeJoin.EdgeDirection direction = edge.direction != null
                ? edge.direction
                : SyncConfig.MergeJoin.EdgeDirection.FROM_TO;
        String sourceField = direction == SyncConfig.MergeJoin.EdgeDirection.FROM_TO ? "_from" : "_to";
        String targetField = direction == SyncConfig.MergeJoin.EdgeDirection.FROM_TO ? "_to" : "_from";
        Map<String, Object> bindVars = Map.of("id", currentId);
        String query = "FOR edge IN " + edge.collection
                + " FILTER edge." + sourceField + " == @id LIMIT 1 RETURN edge." + targetField;
        try (ArangoCursor<String> cursor = arangoDatabase.query(query, bindVars, null, String.class)) {
            if (cursor.hasNext()) {
                return cursor.next();
            }
        } catch (Exception ex) {
            throw new SQLException("Failed to follow connectedEdges for join '" + join.alias
                    + "' using collection " + edge.collection, ex);
        }
        return null;
    }

    private BaseDocument fetchDocumentById(SyncConfig.MergeJoin join, String documentId) throws SQLException {
        if (documentId == null || documentId.isBlank()) {
            return null;
        }
        String[] parts = documentId.split("/", 2);
        if (parts.length != 2) {
            throw new SQLException("Connected edge result '" + documentId + "' is not a valid document handle for join '"
                    + join.alias + "'");
        }
        String collection = parts[0];
        String key = parts[1];
        if (join.collection != null && !join.collection.isBlank() && !collection.equals(join.collection)) {
            throw new SQLException("Connected edge path for join '" + join.alias + "' resolved to collection '" + collection
                    + "', expected '" + join.collection + "'");
        }
        try {
            return arangoDatabase.collection(collection).getDocument(key, BaseDocument.class);
        } catch (ArangoDBException ex) {
            Integer responseCode = ex.getResponseCode();
            Integer errorCode = ex.getErrorNum();
            if ((responseCode != null && responseCode == 404) || (errorCode != null && errorCode == 1202)) {
                return null;
            }
            throw new SQLException("Failed to load document '" + documentId + "' for join '" + join.alias + "'", ex);
        }
    }

    private String buildFieldAccess(String root, String fieldPath) {
        if (fieldPath.startsWith(".")) {
            throw new IllegalArgumentException("Invalid foreign field path: " + fieldPath);
        }
        return root + "." + fieldPath;
    }

    private void ensureDatabase(String databaseName) throws ArangoDBException {
        if (!arangoDB.getDatabases().contains(databaseName)) {
            arangoDB.createDatabase(databaseName);
        }
    }

    private void ensureCollections(SyncConfig cfg) throws SQLException {
        try {
            Set<String> requiredDocuments = new HashSet<>();
            Set<String> requiredEdges = new HashSet<>();
            for (SyncConfig.MergeMapping merge : mergeMappings) {
                if (merge.mainCollection != null && !merge.mainCollection.isBlank()) {
                    requiredDocuments.add(merge.mainCollection);
                }
                for (SyncConfig.MergeJoin join : merge.joins) {
                    if (join.collection != null && !join.collection.isBlank()) {
                        requiredDocuments.add(join.collection);
                    }
                    if (join.connectedEdges != null) {
                        for (SyncConfig.MergeJoin.ConnectedEdge edge : join.connectedEdges) {
                            if (edge.collection != null && !edge.collection.isBlank()) {
                                requiredEdges.add(edge.collection);
                            }
                        }
                    }
                }
            }
            Set<String> existing = new HashSet<>();
            for (CollectionEntity entity : arangoDatabase.getCollections()) {
                existing.add(entity.getName());
            }
            for (String collection : requiredEdges) {
                if (!existing.contains(collection)) {
                    arangoDatabase.createCollection(
                            collection,
                            new CollectionCreateOptions().type(CollectionType.EDGES));
                    existing.add(collection);
                }
            }
            for (String collection : requiredDocuments) {
                if (!existing.contains(collection)) {
                    arangoDatabase.createCollection(collection);
                    existing.add(collection);
                }
            }
        } catch (ArangoDBException ex) {
            throw new SQLException("Failed to ensure ArangoDB collections", ex);
        }
    }

    private boolean exists(TargetTable table, String keyColumn, Object rawKeyValue) throws SQLException {
        String sql = "SELECT 1 FROM " + renderTable(table) + " WHERE " + renderColumn(keyColumn) + " = ? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, toSqlValue(table, keyColumn, rawKeyValue));
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void upsertRow(TargetTable table, String keyColumn, Object rawKeyValue, Map<String, Object> columnValues) throws SQLException {
        if (rawKeyValue == null) {
            throw new SQLException("Null key encountered for table " + displayTable(table));
        }
        Object sqlKeyValue = toSqlValue(table, keyColumn, rawKeyValue);

        Map<String, Object> converted = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : columnValues.entrySet()) {
            converted.put(entry.getKey(), toSqlValue(table, entry.getKey(), entry.getValue()));
        }

        List<String> updateColumns = new ArrayList<>();
        List<Object> updateValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : converted.entrySet()) {
            if (entry.getKey().equals(keyColumn)) {
                continue;
            }
            updateColumns.add(entry.getKey());
            updateValues.add(entry.getValue());
        }

        if (updateColumns.isEmpty()) {
            if (!exists(table, keyColumn, rawKeyValue)) {
                insertRow(table, keyColumn, sqlKeyValue, converted);
            }
            return;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(renderTable(table)).append(" SET ");
        for (int i = 0; i < updateColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(renderColumn(updateColumns.get(i))).append(" = ?");
        }
        sql.append(" WHERE ").append(renderColumn(keyColumn)).append(" = ?");

        try (PreparedStatement update = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Object value : updateValues) {
                update.setObject(index++, value);
            }
            update.setObject(index, sqlKeyValue);
            int affected = update.executeUpdate();
            if (affected == 0) {
                insertRow(table, keyColumn, sqlKeyValue, converted);
            }
        }
    }

    private void insertRow(TargetTable table, String keyColumn, Object sqlKeyValue, Map<String, Object> converted) throws SQLException {
        LinkedHashMap<String, Object> finalColumns = new LinkedHashMap<>();
        if (converted.containsKey(keyColumn)) {
            finalColumns.put(keyColumn, converted.get(keyColumn));
        } else {
            finalColumns.put(keyColumn, sqlKeyValue);
        }
        for (Map.Entry<String, Object> entry : converted.entrySet()) {
            if (!entry.getKey().equals(keyColumn)) {
                finalColumns.put(entry.getKey(), entry.getValue());
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(renderTable(table)).append(" (");
        StringBuilder placeholders = new StringBuilder();
        List<Object> values = new ArrayList<>(finalColumns.size());
        int index = 0;
        for (Map.Entry<String, Object> entry : finalColumns.entrySet()) {
            if (index > 0) {
                sql.append(", ");
                placeholders.append(", ");
            }
            sql.append(renderColumn(entry.getKey()));
            placeholders.append("?");
            Object value = entry.getKey().equals(keyColumn) ? sqlKeyValue : entry.getValue();
            values.add(value);
            index++;
        }
        sql.append(") VALUES (").append(placeholders).append(")");

        try (PreparedStatement insert = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.size(); i++) {
                insert.setObject(i + 1, values.get(i));
            }
            insert.executeUpdate();
        }
    }

    private Object toSqlValue(TargetTable table, String column, Object rawValue) throws SQLException {
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

    private int resolveColumnType(TargetTable table, String column) throws SQLException {
        String tableKey = tableCacheKey(table);
        Map<String, Integer> types = columnTypeCache.get(tableKey);
        if (types == null) {
            types = loadColumnTypes(table);
            columnTypeCache.put(tableKey, types);
        }
        Integer sqlType = types.get(column.toLowerCase(Locale.ROOT));
        return sqlType != null ? sqlType : Types.OTHER;
    }

    private Map<String, Integer> loadColumnTypes(TargetTable table) throws SQLException {
        Map<String, Integer> types = new HashMap<>();
        String tableName = table.table;
        String[] tablePatterns = new String[]{tableName, tableName.toUpperCase(Locale.ROOT), tableName.toLowerCase(Locale.ROOT)};
        for (String pattern : tablePatterns) {
            if (!types.isEmpty()) {
                break;
            }
            try (ResultSet columns = databaseMetaData.getColumns(connection.getCatalog(), schemaPattern(table), pattern, "%")) {
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

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
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
