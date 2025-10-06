package com.example.arango2rdb.view;

import java.util.List;

public class MergeView {

    private final String name;
    private final String mainCollection;
    private final String targetTable;
    private final List<FieldMapping> fieldMappings;
    private final List<JoinMapping> joins;

    public MergeView(String name,
                     String mainCollection,
                     String targetTable,
                     List<FieldMapping> fieldMappings,
                     List<JoinMapping> joins) {
        this.name = name;
        this.mainCollection = mainCollection;
        this.targetTable = targetTable;
        this.fieldMappings = fieldMappings;
        this.joins = joins;
    }

    public String getName() {
        return name;
    }

    public String getMainCollection() {
        return mainCollection;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public List<JoinMapping> getJoins() {
        return joins;
    }

    public record FieldMapping(String source, String target) {
    }

    public record JoinMapping(String alias, String collection, String localField, String foreignField, boolean required) {
    }
}
