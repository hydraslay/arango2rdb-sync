package com.example.arango2rdb.view;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TableSnapshot {

    private final String name;
    private final String encodedName;
    private final String schema;
    private final String domId;
    private final long totalCount;
    private final int size;
    private final List<Map<String, Object>> rows;
    private final List<String> columns;
    private final int step;

    public TableSnapshot(String name,
                         long totalCount,
                         int size,
                         List<Map<String, Object>> rows,
                         List<String> columns,
                         int step) {
        this(name, null, totalCount, size, rows, columns, step);
    }

    public TableSnapshot(String name,
                         String schema,
                         long totalCount,
                         int size,
                         List<Map<String, Object>> rows,
                         List<String> columns,
                         int step) {
        this.name = name;
        this.schema = schema;
        this.encodedName = encode(name);
        this.domId = computeDomId(schema, name);
        this.totalCount = totalCount;
        this.size = size;
        this.rows = rows;
        this.columns = columns;
        this.step = step;
    }

    public String getName() {
        return name;
    }

    public String getEncodedName() {
        return encodedName;
    }

    public String getSchema() {
        return schema;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public int getSize() {
        return size;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isHasMore() {
        return size > 0 && size < totalCount;
    }

    public int getNextSize() {
        if (!isHasMore()) {
            return size;
        }
        long next = (long) size + step;
        return (int) Math.min(next, totalCount);
    }

    public String getDomId() {
        return domId;
    }

    private String encode(String value) {
        try {
            return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
        } catch (Exception ex) {
            return value;
        }
    }

    private String computeDomId(String schema, String value) {
        String base = (schema == null || schema.isBlank()) ? value : schema + "-" + value;
        return "rdb-table-" + slugify(base);
    }

    private String slugify(String value) {
        String slug = value.replaceAll("[^A-Za-z0-9]", "-");
        if (slug.isEmpty()) {
            return "table";
        }
        return slug.toLowerCase(Locale.ROOT);
    }
}
