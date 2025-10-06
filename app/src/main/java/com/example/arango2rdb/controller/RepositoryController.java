package com.example.arango2rdb.controller;

import com.example.arango2rdb.service.RepositoryService;
import com.example.arango2rdb.view.RepositoryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/repositories")
public class RepositoryController {

    private static final Logger log = LoggerFactory.getLogger(RepositoryController.class);

    private final RepositoryService repositoryService;

    public RepositoryController(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    @GetMapping
    public String list(Model model) {
        model.addAttribute("repositories", repositoryService.listRepositories());
        return "repositories";
    }

    @PostMapping
    public String create(@RequestParam("name") String name, RedirectAttributes redirectAttributes) {
        try {
            RepositoryInfo info = repositoryService.createRepository(name);
            redirectAttributes.addFlashAttribute("repositoryStatus", "Repository '" + info.name() + "' created");
        } catch (Exception ex) {
            log.warn("Failed to create repository", ex);
            redirectAttributes.addFlashAttribute("repositoryError", ex.getMessage());
        }
        return "redirect:/repositories";
    }

    @PostMapping("/{id}/delete")
    public String delete(@PathVariable("id") long id, RedirectAttributes redirectAttributes) {
        try {
            repositoryService.deleteRepository(id);
            redirectAttributes.addFlashAttribute("repositoryStatus", "Repository deleted");
        } catch (Exception ex) {
            log.warn("Failed to delete repository {}", id, ex);
            redirectAttributes.addFlashAttribute("repositoryError", ex.getMessage());
        }
        return "redirect:/repositories";
    }

    @PostMapping("/{id}/commit")
    public String commit(@PathVariable("id") long id, RedirectAttributes redirectAttributes) {
        try {
            repositoryService.markCommitted(id);
            redirectAttributes.addFlashAttribute("repositoryStatus", "Repository marked as committed");
        } catch (Exception ex) {
            log.warn("Failed to mark repository {} as committed", id, ex);
            redirectAttributes.addFlashAttribute("repositoryError", ex.getMessage());
        }
        return "redirect:/repositories";
    }

    @GetMapping("/{id}/open")
    public String open(@PathVariable("id") long id, RedirectAttributes redirectAttributes) {
        redirectAttributes.addAttribute("repo", id);
        return "redirect:/";
    }
}
