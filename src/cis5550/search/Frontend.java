package cis5550.search;

import static cis5550.webserver.Server.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import cis5550.tools.Logger;

public class Frontend {
    static final Logger logger = Logger.getLogger(Frontend.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Frontend <port> <kvsCoordinator>");
            System.err.println("Example: Frontend 8080 localhost:8000");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String kvsCoordinator = args[1];

        logger.info("Starting search frontend on port " + port);
        logger.info("KVS coordinator: " + kvsCoordinator);

        // Initialize ranker
        Ranker ranker = new Ranker(kvsCoordinator);

        // Configure server - enable HTTPS on port 443 if running on port 80
        if (port == 80) {
            securePort(443);
        }
        port(port);

        // Serve index.html for root path
        get("/", (req, res) -> {
            try {
                String content = new String(Files.readAllBytes(Paths.get("static/index.html")));
                res.type("text/html");
                return content;
            } catch (IOException e) {
                res.status(500, "Internal Server Error");
                return "Could not load index.html";
            }
        });

        // Search API endpoint
        get("/search", (req, res) -> {
            String query = req.queryParams("q");
            logger.info("Search query: " + query);

            if (query == null || query.trim().isEmpty()) {
                res.type("application/json");
                return "{\"results\":[],\"error\":\"Empty query\"}";
            }

            try {
                List<Ranker.SearchResult> results = ranker.search(query);
                res.type("application/json");
                return Ranker.resultsToJson(results);
            } catch (Exception e) {
                logger.error("Search error", e);
                res.status(500, "Internal Server Error");
                res.type("application/json");
                return "{\"results\":[],\"error\":\"" + e.getMessage() + "\"}";
            }
        });

        // Health check endpoint
        get("/health", (req, res) -> {
            res.type("text/plain");
            return "OK";
        });

        logger.info("Search frontend started successfully");
    }
}
