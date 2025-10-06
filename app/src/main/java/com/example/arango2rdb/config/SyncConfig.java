package com.example.arango2rdb.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SyncConfig {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern FIELD_PATH = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z0-9_]+)+");

    public ArangoConfig arango;
    public RdbConfig rdb;
    public List<CollectionMapping> collections = Collections.emptyList();
    public List<MergeMapping> merges = Collections.emptyList();

    public void validate() {
        if (arango == null) {
            throw new IllegalArgumentException("Missing ArangoDB configuration");
        }
        arango.validate();
        if (rdb == null) {
            throw new IllegalArgumentException("Missing relational database configuration");
        }
        rdb.validate();
        if (collections == null || collections.isEmpty()) {
            throw new IllegalArgumentException("At least one collection mapping is required");
        }

        Map<String, CollectionMapping> byTable = new HashMap<>();
        for (CollectionMapping mapping : collections) {
            mapping.validate();
            String tableKey = mapping.tableKey();
            if (byTable.put(tableKey, mapping) != null) {
                throw new IllegalArgumentException("Duplicate mapping for table " + mapping.table);
            }
        }

        for (CollectionMapping mapping : collections) {
            for (String dependency : mapping.dependsOn) {
                String depKey = dependency.toLowerCase(Locale.ROOT);
                if (!byTable.containsKey(depKey)) {
                    throw new IllegalArgumentException(
                            "Unknown dependency '" + dependency + "' declared for table " + mapping.table);
                }
                if (depKey.equals(mapping.tableKey())) {
                    throw new IllegalArgumentException(
                            "Table " + mapping.table + " cannot depend on itself");
                }
            }
        }

        if (merges == null) {
            merges = Collections.emptyList();
        }
        Set<String> mergeNames = new HashSet<>();
        for (MergeMapping merge : merges) {
            merge.validate(null, byTable.keySet());
            if (!mergeNames.add(merge.name)) {
                throw new IllegalArgumentException("Duplicate merge mapping name " + merge.name);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ArangoConfig {
        public String host = "localhost";
        public int port = 8529;
        public String user = "root";
        public String password = "";
        public String database;
        public boolean useSsl = false;

        void validate() {
            if (database == null || database.isBlank()) {
                throw new IllegalArgumentException("ArangoDB database name is required");
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RdbConfig {
        public String jdbcUrl;
        public String user;
        public String password;

        void validate() {
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                throw new IllegalArgumentException("Relational database JDBC URL is required");
            }
            if (user == null) {
                throw new IllegalArgumentException("Relational database user is required");
            }
            if (password == null) {
                throw new IllegalArgumentException("Relational database password is required");
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CollectionMapping {
        public String collection;
        public String table;
        public String keyField = "_key";
        public String keyColumn = "id";
        public Map<String, String> fieldMappings = Collections.emptyMap();
        public List<String> dependsOn = Collections.emptyList();

        void validate() {
            if (collection == null || collection.isBlank()) {
                throw new IllegalArgumentException("Mapping collection name is required");
            }
            if (table == null || table.isBlank()) {
                throw new IllegalArgumentException("Mapping table name is required");
            }
            if (keyField == null || keyField.isBlank()) {
                throw new IllegalArgumentException("Mapping key field is required");
            }
            if (keyColumn == null || keyColumn.isBlank()) {
                throw new IllegalArgumentException("Mapping key column is required");
            }
            if (fieldMappings == null) {
                throw new IllegalArgumentException(
                        "Field mappings must be provided for collection " + collection);
            }
            if (dependsOn == null) {
                dependsOn = Collections.emptyList();
            } else {
                List<String> cleaned = new ArrayList<>();
                for (String dependency : dependsOn) {
                    if (dependency == null || dependency.isBlank()) {
                        throw new IllegalArgumentException(
                                "Dependencies for table " + table + " must not contain blank entries");
                    }
                    String trimmed = dependency.trim();
                    if (!cleaned.contains(trimmed)) {
                        cleaned.add(trimmed);
                    }
                }
                dependsOn = List.copyOf(cleaned);
            }
        }

        String tableKey() {
            return table.toLowerCase(Locale.ROOT);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MergeMapping {
        public String name;
        public String targetTable;
        public String mainCollection;
        public String keyColumn;
        public String keyField;
        public Map<String, String> fieldMappings = Collections.emptyMap();
        public List<MergeJoin> joins = Collections.emptyList();

        void validate(Set<String> knownCollections, Set<String> knownTables) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Merge mapping name is required");
            }
            if (!IDENTIFIER.matcher(name).matches()) {
                throw new IllegalArgumentException("Merge mapping name contains invalid characters: " + name);
            }
            if (targetTable == null || targetTable.isBlank()) {
                throw new IllegalArgumentException("Merge mapping target table is required for " + name);
            }
            if (mainCollection == null || mainCollection.isBlank()) {
                throw new IllegalArgumentException("Merge mapping main collection is required for " + name);
            }
            if (knownCollections != null && !knownCollections.contains(mainCollection)) {
                throw new IllegalArgumentException(
                        "Merge mapping " + name + " references unknown collection " + mainCollection);
            }
            if (keyColumn == null || keyColumn.isBlank()) {
                throw new IllegalArgumentException("Merge mapping key column is required for " + name);
            }
            if (keyField == null || keyField.isBlank()) {
                throw new IllegalArgumentException("Merge mapping key field is required for " + name);
            }
            if (!FIELD_PATH.matcher(keyField).matches()) {
                throw new IllegalArgumentException(
                        "Merge mapping key field must reference an alias property (alias.field): " + keyField);
            }
            if (fieldMappings == null || fieldMappings.isEmpty()) {
                throw new IllegalArgumentException("Merge mapping " + name + " must define field mappings");
            }

            Map<String, String> cleanedFields = new HashMap<>();
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                String source = entry.getKey();
                String target = entry.getValue();
                if (source == null || source.isBlank()) {
                    throw new IllegalArgumentException(
                            "Merge mapping " + name + " contains a blank field source");
                }
                if (!FIELD_PATH.matcher(source).matches()) {
                    throw new IllegalArgumentException(
                            "Merge mapping " + name + " field source must be alias.property: " + source);
                }
                if (target == null || target.isBlank()) {
                    throw new IllegalArgumentException(
                            "Merge mapping " + name + " contains a blank field target for " + source);
                }
                cleanedFields.put(source, target);
            }
            fieldMappings = Map.copyOf(cleanedFields);
            if (!fieldMappings.containsKey(keyField)) {
                throw new IllegalArgumentException(
                        "Merge mapping " + name + " must map keyField " + keyField + " to a target column");
            }

            if (joins == null) {
                joins = Collections.emptyList();
            }
            Map<String, MergeJoin> aliases = new HashMap<>();
            List<MergeJoin> cleanedJoins = new ArrayList<>();
            for (MergeJoin join : joins) {
                join.validate(name, aliases.keySet(), knownCollections, knownTables);
                aliases.put(join.alias, join);
                cleanedJoins.add(join);
            }
            joins = List.copyOf(cleanedJoins);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MergeJoin {
        public String alias;
        public String collection;
        public String localField;
        public String foreignField;
        public boolean required = true;

        void validate(String mergeName,
                       Set<String> existingAliases,
                       Set<String> knownCollections,
                       Set<String> knownTables) {
            if (alias == null || alias.isBlank()) {
                throw new IllegalArgumentException("Join alias is required for merge " + mergeName);
            }
            if (!IDENTIFIER.matcher(alias).matches()) {
                throw new IllegalArgumentException(
                        "Join alias contains invalid characters in merge " + mergeName + ": " + alias);
            }
            if ("main".equalsIgnoreCase(alias)) {
                throw new IllegalArgumentException("Join alias cannot be 'main' in merge " + mergeName);
            }
            if (existingAliases.contains(alias)) {
                throw new IllegalArgumentException(
                        "Duplicate join alias '" + alias + "' in merge " + mergeName);
            }
            if (collection == null || collection.isBlank()) {
                throw new IllegalArgumentException(
                        "Join collection is required for alias '" + alias + "' in merge " + mergeName);
            }
            if (knownCollections != null && !knownCollections.contains(collection)) {
                throw new IllegalArgumentException(
                        "Merge " + mergeName + " references unknown collection " + collection + " for alias " + alias);
            }
            if (localField == null || localField.isBlank()) {
                throw new IllegalArgumentException(
                        "Join localField is required for alias '" + alias + "' in merge " + mergeName);
            }
            if (!FIELD_PATH.matcher(localField).matches()) {
                throw new IllegalArgumentException(
                        "Join localField must be alias.property for alias '" + alias + "' in merge " + mergeName);
            }
            if (foreignField == null || foreignField.isBlank()) {
                throw new IllegalArgumentException(
                        "Join foreignField is required for alias '" + alias + "' in merge " + mergeName);
            }
            if (!IDENTIFIER.matcher(foreignField.replace(".", "_")).matches()) {
                throw new IllegalArgumentException(
                        "Join foreignField contains invalid characters for alias '" + alias + "' in merge " + mergeName);
            }
        }
    }
}

