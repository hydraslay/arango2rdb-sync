package com.example.arango2rdb.view;

import java.util.List;

public class MappingView {

    private final String collectionName;
    private final String tableName;
    private final List<FieldMapping> fieldMappings;

    public MappingView(String collectionName, String tableName, List<FieldMapping> fieldMappings) {
        this.collectionName = collectionName;
        this.tableName = tableName;
        this.fieldMappings = fieldMappings;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public record FieldMapping(String source, String target) {
    }
}
