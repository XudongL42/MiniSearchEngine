package com.minisearch.api;

import com.minisearch.model.IndexRequest;
import com.minisearch.model.SearchRequest;
import com.minisearch.model.SearchResponse;
import io.javalin.Javalin;
import io.javalin.http.Context;

public class SearchServer {

    private final SearchService service;
    private final Javalin       app;

    public SearchServer(int port) throws Exception {
        service = new SearchService();

        app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors ->
                    cors.addRule(it -> it.anyHost()));
            config.jsonMapper(new io.javalin.json.JavalinJackson());
        });

        registerRoutes();
        app.start(port);
        System.out.printf("Search API running on http://localhost:%d%n", port);
        printUsage(port);
    }

    private void registerRoutes() {

        // ---------------------------------------------------------------
        // GET /search — main search endpoint
        // ---------------------------------------------------------------
        app.get("/search", ctx -> {
            try {
                SearchRequest  req      = SearchRequest.fromContext(ctx);
                SearchResponse response = service.search(req);
                ctx.json(response);
            } catch (Exception e) {
                ctx.status(400).json(errorBody(e.getMessage()));
            }
        });

        // ---------------------------------------------------------------
        // POST /index — add a document
        // ---------------------------------------------------------------
        app.post("/index", ctx -> {
            try {
                IndexRequest req    = ctx.bodyAsClass(IndexRequest.class);
                String validationError = req.validate();
                if (validationError != null) {
                    ctx.status(400).json(errorBody(validationError));
                    return;
                }
                String id = service.indexDocument(req);
                ctx.status(201).json(java.util.Map.of(
                        "id",     id,
                        "status", "indexed"));
            } catch (Exception e) {
                ctx.status(500).json(errorBody(e.getMessage()));
            }
        });

        // ---------------------------------------------------------------
        // DELETE /index/:id — remove a document
        // ---------------------------------------------------------------
        app.delete("/index/{id}", ctx -> {
            try {
                String id = ctx.pathParam("id");
                service.deleteDocument(id);
                ctx.json(java.util.Map.of(
                        "id",     id,
                        "status", "deleted"));
            } catch (Exception e) {
                ctx.status(500).json(errorBody(e.getMessage()));
            }
        });

        // ---------------------------------------------------------------
        // GET /stats — index statistics
        // ---------------------------------------------------------------
        app.get("/stats", ctx -> {
            try {
                ctx.json(service.stats());
            } catch (Exception e) {
                ctx.status(500).json(errorBody(e.getMessage()));
            }
        });

        // ---------------------------------------------------------------
        // GET /health — liveness check
        // ---------------------------------------------------------------
        app.get("/health", ctx ->
                ctx.json(java.util.Map.of("status", "ok")));

        // Graceful shutdown on SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            try {
                app.stop();
                service.close();
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));
    }

    private java.util.Map<String, String> errorBody(String message) {
        return java.util.Map.of("error", message != null ? message : "unknown error");
    }

    private void printUsage(int port) {
        System.out.println("\\nExample curl commands:");
        System.out.printf("  curl 'http://localhost:%d/health'%n", port);
        System.out.printf("  curl 'http://localhost:%d/stats'%n", port);
        System.out.printf("  curl 'http://localhost:%d/search?q=lucene'%n", port);
        System.out.printf("  curl 'http://localhost:%d/search?q=search&category=TECHNOLOGY&sort=date_desc'%n", port);
        System.out.printf("  curl 'http://localhost:%d/search?q=index&explain=true'%n", port);
        System.out.printf("  curl -X POST 'http://localhost:%d/index' \\\\%n", port);
        System.out.println("       -H 'Content-Type: application/json' \\\\");
        System.out.println("       -d '{\"title\":\"My Article\",\"description\":\"About Lucene search\",\"category\":\"TECHNOLOGY\"}'");
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7070;
        new SearchServer(port);
        // Block main thread — Javalin runs on its own threads
        Thread.currentThread().join();
    }
}
