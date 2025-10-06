package com.example.arango2rdb.config;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionEntity;
import com.example.arango2rdb.MappingLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

@Configuration
public class AppConfiguration {

    private static final int MIN_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 10;

    @Value("${app.mapping.path:config/mapping.json}")
    private String mappingPath;

    @Bean
    public MappingLoader mappingLoader() {
        return new MappingLoader();
    }

    @Bean
    public SyncConfig syncConfig(MappingLoader loader) throws IOException {
        Path path = Path.of(mappingPath);
        if (!Files.exists(path)) {
            throw new IOException("Mapping configuration not found at " + path.toAbsolutePath());
        }
        SyncConfig config = loader.load(path);
        config.validate();
        return config;
    }

    @Bean
    public ArangoDB arangoDB(SyncConfig config) {
        SyncConfig.ArangoConfig arango = config.arango;
        ArangoDB.Builder builder = new ArangoDB.Builder()
                .host(arango.host, arango.port)
                .user(arango.user)
                .password(arango.password);
        if (arango.useSsl) {
            builder.useSsl(true);
        }
        return builder.build();
    }

    @Bean
    public ArangoDatabase arangoDatabase(ArangoDB arangoDB, SyncConfig config) {
        ensureDatabase(arangoDB, config);
        ArangoDatabase database = arangoDB.db(config.arango.database);
        ensureCollections(database, config);
        return database;
    }

    @Bean
    public DataSource dataSource(SyncConfig config) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.rdb.jdbcUrl);
        dataSource.setUsername(config.rdb.user);
        dataSource.setPassword(config.rdb.password);
        dataSource.setMinimumIdle(MIN_POOL_SIZE);
        dataSource.setMaximumPoolSize(MAX_POOL_SIZE);
        dataSource.setPoolName("rdb-pool");
        return dataSource;
    }

    private void ensureDatabase(ArangoDB arangoDB, SyncConfig config) {
        String databaseName = config.arango.database;
        if (databaseName == null || databaseName.isBlank()) {
            throw new IllegalArgumentException("Arango database name must be provided");
        }
        Set<String> existing = new HashSet<>(arangoDB.getDatabases());
        if (!existing.contains(databaseName)) {
            arangoDB.createDatabase(databaseName);
        }
    }

    private void ensureCollections(ArangoDatabase database, SyncConfig config) {
        Set<String> required = new HashSet<>();
        if (config.merges != null) {
            for (SyncConfig.MergeMapping merge : config.merges) {
                required.add(merge.mainCollection);
                merge.joins.forEach(join -> required.add(join.collection));
            }
        }
        Set<String> existing = new HashSet<>();
        for (CollectionEntity entity : database.getCollections()) {
            existing.add(entity.getName().toLowerCase(Locale.ROOT));
        }
        for (String name : required) {
            if (name == null || name.isBlank()) {
                continue;
            }
            String lookup = name.toLowerCase(Locale.ROOT);
            if (!existing.contains(lookup)) {
                database.createCollection(name);
            }
        }
    }
}
