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

        if (merges == null || merges.isEmpty()) {
            throw new IllegalArgumentException("At least one merge mapping is required");
        }

        Set<String> mergeNames = new HashSet<>();
        Set<String> targetTables = new HashSet<>();
        List<MergeMapping> cleaned = new ArrayList<>(merges.size());
        for (MergeMapping merge : merges) {
            merge.validate();
            if (!mergeNames.add(merge.name)) {
                throw new IllegalArgumentException("Duplicate merge mapping name " + merge.name);
            }
            String tableKey = merge.targetTable.toLowerCase(Locale.ROOT);
            if (!targetTables.add(tableKey)) {
                throw new IllegalArgumentException("Duplicate merge target table " + merge.targetTable);
            }
            cleaned.add(merge);
        }
        merges = List.copyOf(cleaned);
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
    public static class MergeMapping {
        public String name;
        public String targetTable;
        public String mainCollection;
        public String keyColumn;
        public String keyField;
        public Map<String, String> fieldMappings = Collections.emptyMap();
        public List<MergeJoin> joins = Collections.emptyList();

        void validate() {
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
                join.validate(name, aliases.keySet());
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

        void validate(String mergeName, Set<String> existingAliases) {
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
