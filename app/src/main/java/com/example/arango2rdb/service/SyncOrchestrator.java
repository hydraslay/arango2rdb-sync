package com.example.arango2rdb.service;

import com.example.arango2rdb.SyncService;
import com.example.arango2rdb.config.SyncConfig;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class SyncOrchestrator {

    private final SyncConfig config;

    public SyncOrchestrator(SyncConfig config) {
        this.config = config;
    }

    public void runSync(String repositorySchema) {
        try (SyncService service = new SyncService(config)) {
            service.run(repositorySchema);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to run sync", ex);
        }
    }
}
