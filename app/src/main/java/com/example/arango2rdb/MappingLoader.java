package com.example.arango2rdb;

import com.example.arango2rdb.config.SyncConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class MappingLoader {
    private final ObjectMapper mapper;

    public MappingLoader() {
        this.mapper = new ObjectMapper();
        this.mapper.findAndRegisterModules();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public SyncConfig load(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return read(in);
        }
    }

    public SyncConfig read(InputStream stream) throws IOException {
        SyncConfig config = mapper.readValue(stream, SyncConfig.class);
        config.validate();
        return config;
    }
}
