package com.example.arango2rdb.controller;

import com.example.arango2rdb.service.RepositoryService;
import com.example.arango2rdb.service.SyncOrchestrator;
import com.example.arango2rdb.service.VisualizationService;
import com.example.arango2rdb.view.CollectionSnapshot;
import com.example.arango2rdb.view.RepositoryInfo;
import com.example.arango2rdb.view.TableSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Optional;

@Controller
public class SchemaController {

    private static final Logger log = LoggerFactory.getLogger(SchemaController.class);

    private final VisualizationService service;
    private final RepositoryService repositoryService;
    private final SyncOrchestrator syncOrchestrator;

    public SchemaController(VisualizationService service,
                            RepositoryService repositoryService,
                            SyncOrchestrator syncOrchestrator) {
        this.service = service;
        this.repositoryService = repositoryService;
        this.syncOrchestrator = syncOrchestrator;
    }

    @GetMapping("/")
    public String index(@RequestParam(name = "filter", required = false) String filter,
                        @RequestParam(name = "repo", required = false) Optional<Long> repositoryId,
                        Model model) {
        Optional<RepositoryInfo> activeRepository = repositoryId.flatMap(repositoryService::findById);
        String schema = activeRepository.map(RepositoryInfo::schemaName).orElse(null);

        model.addAttribute("filter", filter);
        model.addAttribute("repositories", repositoryService.listRepositories());
        model.addAttribute("activeRepository", activeRepository.orElse(null));
        model.addAttribute("activeRepositoryId", activeRepository.map(RepositoryInfo::id).orElse(null));
        model.addAttribute("arangoCollections", service.loadArangoCollections(filter));
        model.addAttribute("rdbTables", service.loadTables(filter, schema));
        model.addAttribute("merges", service.getMergeMappings());
        return "schema-view";
    }

    @GetMapping("/arango/{collection}")
    public String arangoFragment(@PathVariable("collection") String collection,
                                 @RequestParam(name = "size", required = false) Optional<Long> size,
                                 @RequestParam(name = "repo", required = false) Optional<Long> repositoryId,
                                 Model model) {
        CollectionSnapshot snapshot = service.loadArangoCollection(collection, size.map(this::safeInt).orElse(-1));
        model.addAttribute("col", snapshot);
        return "fragments/arango-collection :: collectionFragment";
    }

    @GetMapping("/rdb/{table}")
    public String rdbFragment(@PathVariable("table") String table,
                              @RequestParam(name = "size", required = false) Optional<Long> size,
                              @RequestParam(name = "repo", required = false) Optional<Long> repositoryId,
                              Model model) {
        Optional<RepositoryInfo> activeRepository = repositoryId.flatMap(repositoryService::findById);
        String schema = activeRepository.map(RepositoryInfo::schemaName).orElse(null);
        TableSnapshot snapshot = service.loadTable(table, size.map(this::safeInt).orElse(-1), schema);
        model.addAttribute("table", snapshot);
        model.addAttribute("repoId", activeRepository.map(RepositoryInfo::id).orElse(null));
        return "fragments/rdb-table :: table";
    }

    @PostMapping("/sync")
    public String sync(@RequestParam(name = "repo", required = false) Optional<Long> repositoryId,
                       RedirectAttributes redirectAttributes) {
        Optional<RepositoryInfo> activeRepository = repositoryId.flatMap(repositoryService::findById);
        if (repositoryId.isPresent() && activeRepository.isEmpty()) {
            redirectAttributes.addFlashAttribute("syncError", "Repository not found");
            return "redirect:/repositories";
        }
        try {
            syncOrchestrator.runSync(activeRepository.map(RepositoryInfo::schemaName).orElse(null));
            redirectAttributes.addFlashAttribute("syncStatus", "Sync completed successfully.");
        } catch (Exception ex) {
            log.error("Sync execution failed", ex);
            redirectAttributes.addFlashAttribute("syncError", ex.getMessage());
        }
        activeRepository.ifPresent(repo -> redirectAttributes.addAttribute("repo", repo.id()));
        return "redirect:/";
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
