package com.example.arango2rdb.view;

import java.time.Instant;

public record RepositoryInfo(long id,
                             String name,
                             String schemaName,
                             String status,
                             Instant createdAt) {
}
