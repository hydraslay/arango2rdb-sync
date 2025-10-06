package com.example.arango2rdb.controller;

import com.example.arango2rdb.service.VisualizationService;
import com.example.arango2rdb.view.CollectionSnapshot;
import com.example.arango2rdb.view.TableSnapshot;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Optional;

@Controller
public class SchemaController {

    private final VisualizationService service;

    public SchemaController(VisualizationService service) {
        this.service = service;
    }

    @GetMapping("/")
    public String index(@RequestParam(name = "filter", required = false) String filter,
                        Model model) {
        model.addAttribute("filter", filter);
        model.addAttribute("arangoCollections", service.loadArangoCollections(filter));
        model.addAttribute("rdbTables", service.loadTables(filter));
        model.addAttribute("mappings", service.getCollectionMappings());
        model.addAttribute("merges", service.getMergeMappings());
        return "schema-view";
    }

    @GetMapping("/arango/{collection}")
    public String arangoFragment(@PathVariable("collection") String collection,
                                 @RequestParam(name = "size", required = false) Optional<Long> size,
                                 Model model) {
        CollectionSnapshot snapshot = service.loadArangoCollection(collection, size.map(this::safeInt).orElse(-1));
        model.addAttribute("col", snapshot);
        return "fragments/arango-collection :: collectionFragment";
    }

    @GetMapping("/rdb/{table}")
    public String rdbFragment(@PathVariable("table") String table,
                              @RequestParam(name = "size", required = false) Optional<Long> size,
                              Model model) {
        TableSnapshot snapshot = service.loadTable(table, size.map(this::safeInt).orElse(-1));
        model.addAttribute("table", snapshot);
        return "fragments/rdb-table :: table";
    }

    private int safeInt(long value) {
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) value;
    }
}
