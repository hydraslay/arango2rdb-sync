package com.example.arango2rdb;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.example.arango2rdb.config.SyncConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class App {
    private static final String DEFAULT_CONFIG = "config/mapping.json";

    private App() {
    }

    public static void main(String[] args) {
        String command = args.length > 0 ? args[0] : "sync";
        String configPathArg = determineConfigPath(args, command);
        Path configPath = Paths.get(configPathArg);

        if (!Files.exists(configPath)) {
            System.err.printf(Locale.US, "Configuration file not found: %s%n", configPath.toAbsolutePath());
            System.exit(1);
        }

        MappingLoader loader = new MappingLoader();
        try {
            SyncConfig config = loader.load(configPath);
            switch (command) {
                case "sync":
                    runSync(config);
                    break;
                case "describe-arango":
                    describeArango(config);
                    break;
                case "describe-rdb":
                    describeRdb(config);
                    break;
                case "help":
                    printUsage();
                    break;
                default:
                    System.err.printf(Locale.US, "Unknown command: %s%n", command);
                    printUsage();
                    System.exit(1);
            }
        } catch (Exception ex) {
            System.err.printf(Locale.US, "Operation failed: %s%n", ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static String determineConfigPath(String[] args, String command) {
        if (args.length == 0) {
            return DEFAULT_CONFIG;
        }
        if (args.length == 1) {
            return DEFAULT_CONFIG;
        }
        if ("help".equals(command)) {
            return DEFAULT_CONFIG;
        }
        return args[1];
    }

    private static void runSync(SyncConfig config) throws SQLException {
        try (SyncService service = new SyncService(config)) {
            service.run();
            System.out.println("Sync completed successfully.");
        }
    }

    private static void describeArango(SyncConfig config) {
        SyncConfig.ArangoConfig arango = config.arango;
        ArangoDB.Builder builder = new ArangoDB.Builder()
                .host(arango.host, arango.port)
                .user(arango.user)
                .password(arango.password);
        if (arango.useSsl) {
            builder.useSsl(true);
        }
        ObjectMapper mapper = new ObjectMapper();

        ArangoDB arangoDB = builder.build();
        try {
            ArangoDatabase database = arangoDB.db(arango.database);
            System.out.printf(Locale.US, "Collections in database %s:%n", arango.database);
            for (CollectionEntity entity : database.getCollections()) {
                if (Boolean.TRUE.equals(entity.getIsSystem())) {
                    continue;
                }
                ArangoCollection collection = database.collection(entity.getName());
                long count = 0L;
                try {
                    count = collection.count().getCount();
                } catch (Exception ignored) {
                    // Counting might require elevated permissions; ignore failures.
                }
                System.out.printf(Locale.US, "- %s (approx. %d docs)%n", entity.getName(), count);
                try (ArangoCursor<BaseDocument> cursor = database.query(
                        "FOR doc IN @@collection LIMIT 1 RETURN doc",
                        Map.of("@collection", entity.getName()),
                        null,
                        BaseDocument.class)) {
                    if (cursor.hasNext()) {
                        BaseDocument sample = cursor.next();
                        Map<String, Object> properties = sample.getProperties();
                        String json = toJson(mapper, properties);
                        System.out.printf(Locale.US, "  sample: %s%n", json);
                    } else {
                        System.out.println("  sample: <empty>");
                    }
                } catch (Exception ignored) {
                    System.out.println("  sample: <unavailable>");
                }
            }
        } finally {
            arangoDB.shutdown();
        }
    }

    private static String toJson(ObjectMapper mapper, Map<String, Object> properties) {
        try {
            return mapper.writeValueAsString(properties);
        } catch (JsonProcessingException e) {
            return properties.toString();
        }
    }

    private static void describeRdb(SyncConfig config) throws SQLException {
        SyncConfig.RdbConfig rdb = config.rdb;
        try (Connection connection = DriverManager.getConnection(rdb.jdbcUrl, rdb.user, rdb.password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.printf(Locale.US, "Tables in %s:%n", rdb.jdbcUrl);
            try (ResultSet tables = metaData.getTables(connection.getCatalog(), null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.printf(Locale.US, "- %s%n", tableName);
                    printColumns(metaData, tableName);
                }
            }
        }
    }

    private static void printColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        Set<String> printed = new HashSet<>();
        try (ResultSet columns = metaData.getColumns(null, null, tableName, "%")) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                if (!printed.add(columnName)) {
                    continue;
                }
                String typeName = columns.getString("TYPE_NAME");
                int size = columns.getInt("COLUMN_SIZE");
                String nullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls ? "NOT NULL" : "NULLABLE";
                System.out.printf(Locale.US, "  %s %s(%d) %s%n", columnName, typeName, size, nullable);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar arango2rdb-sync.jar <command> [configPath]");
        System.out.println("Commands:");
        System.out.println("  sync [configPath]            Sync data from ArangoDB to the relational DB");
        System.out.println("  describe-arango [configPath] Print ArangoDB collections with a sample document");
        System.out.println("  describe-rdb [configPath]    Print relational database tables and columns");
        System.out.println("  help                         Show this message");
        System.out.printf(Locale.US, "Default config path: %s%n", DEFAULT_CONFIG);
    }
}
