package com.example.arango2rdb.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SyncConfig {
    public ArangoConfig arango;
    public RdbConfig rdb;
    public List<CollectionMapping> collections = Collections.emptyList();

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
        for (CollectionMapping mapping : collections) {
            mapping.validate();
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
                throw new IllegalArgumentException("Field mappings must be provided for collection " + collection);
            }
        }
    }
}
