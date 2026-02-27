package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import org.apache.commons.text.StringEscapeUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {
    static final Logger logger = Logger.getLogger(PageRank.class);

    static final String CRAWL_TABLE = Crawler.CRAWL_TABLE;
    static final String LINKS_TABLE = "pt-links";
    static final String PAGERANK_TABLE = "pt-pagerank";

    // PageRank parameters
    static final double DAMPING_FACTOR = 0.85;
    static final double CONVERGENCE_THRESHOLD_PER_PAGE = 0.00005;
    static final int MAX_ITERATIONS = 50;
    static final int BATCH_SIZE = 1000;

    // Pattern to match anchor tags with href
    static final Pattern ANCHOR_PATTERN = Pattern.compile(
            "<a\\s+[^>]*href\\s*=\\s*[\"']([^\"']*)[\"'][^>]*>",
            Pattern.CASE_INSENSITIVE);

    public static void run(FlameContext ctx, String[] args) throws Exception {
        KVSClient kvs = ctx.getKVS();
        long startTime = System.currentTimeMillis();

        int pageCount = kvs.count(CRAWL_TABLE);
        ctx.output("Starting PageRank with " + pageCount + " pages from " + CRAWL_TABLE + "\n");
        ctx.output("Parameters: damping=" + DAMPING_FACTOR + ", convergencePerPage=" + CONVERGENCE_THRESHOLD_PER_PAGE +
                ", maxIterations=" + MAX_ITERATIONS + "\n\n");

        // Delete existing tables if they exist
        if (kvs.count(LINKS_TABLE) > 0) {
            ctx.output("Deleting existing " + LINKS_TABLE + " table...\n");
            kvs.delete(LINKS_TABLE);
        }
        if (kvs.count(PAGERANK_TABLE) > 0) {
            ctx.output("Deleting existing " + PAGERANK_TABLE + " table...\n");
            kvs.delete(PAGERANK_TABLE);
        }

        final String kvsCoordinator = kvs.getCoordinator();

        // Step 1: Build the link graph using mapPartitions
        // Workers build corpus set once per partition and write links directly to KVS
        ctx.output("Step 1: Building link graph...\n");

        FlameRDD crawlKeys = ctx.fromTable(CRAWL_TABLE, row -> row.key());
        ctx.output("Loaded " + crawlKeys.count() + " row keys\n");

        FlameRDD linkResults = crawlKeys.mapPartitions(keyIterator -> {
            KVSClient workerKvs = new KVSClient(kvsCoordinator);
            workerKvs.numWorkers(); // Pre-initialize worker list

            // Build corpus set ONCE per partition by scanning CRAWL_TABLE
            Set<String> corpus = new HashSet<>();
            Iterator<Row> corpusIter = workerKvs.scan(CRAWL_TABLE);
            while (corpusIter.hasNext()) {
                Row row = corpusIter.next();
                String url = row.get("url");
                if (url != null) {
                    corpus.add(url);
                }
            }

            List<Row> linkBatch = new ArrayList<>();
            int processed = 0;

            while (keyIterator.hasNext()) {
                String hashedUrl = keyIterator.next();
                Row row = workerKvs.getRow(CRAWL_TABLE, hashedUrl);
                if (row == null) continue;

                String url = row.get("url");
                String content = row.get("content");
                if (url == null || content == null) continue;

                // Unescape HTML and extract links
                String unescaped = content;
                String prev;
                do {
                    prev = unescaped;
                    unescaped = StringEscapeUtils.unescapeHtml4(unescaped);
                } while (!unescaped.equals(prev));

                List<String> outlinks = extractLinks(unescaped, url, corpus);

                // Store to pt-links table
                Row linkRow = new Row(Hasher.hash(url));
                linkRow.put("url", url);
                linkRow.put("outlinks", String.join(",", outlinks));
                linkBatch.add(linkRow);
                processed++;

                if (linkBatch.size() >= BATCH_SIZE) {
                    workerKvs.putRowBatch(LINKS_TABLE, linkBatch, false);
                    linkBatch.clear();
                }
            }

            // Write remaining batch
            if (!linkBatch.isEmpty()) {
                workerKvs.putRowBatch(LINKS_TABLE, linkBatch, false);
            }

            List<String> stats = new ArrayList<>();
            stats.add("processed:" + processed);
            return stats.iterator();
        });

        // Collect stats
        List<String> stats = linkResults.collect();
        int totalProcessed = 0;
        for (String stat : stats) {
            if (stat.startsWith("processed:")) {
                totalProcessed += Integer.parseInt(stat.substring("processed:".length()));
            }
        }
        ctx.output("Link extraction complete: " + totalProcessed + " pages processed\n");

        // Clean up
        crawlKeys.destroy();
        linkResults.destroy();

        // Step 2: Load link graph from KVS for iterations
        ctx.output("\nStep 2: Loading link graph for iterations...\n");

        Map<String, String> linkGraphMap = new HashMap<>();
        Map<String, Double> rankMap = new HashMap<>();

        Iterator<Row> linkIter = kvs.scan(LINKS_TABLE);
        while (linkIter.hasNext()) {
            Row row = linkIter.next();
            String url = row.get("url");
            String outlinks = row.get("outlinks");
            if (url != null) {
                linkGraphMap.put(url, outlinks != null ? outlinks : "");
                rankMap.put(url, 1.0);
            }
        }

        int corpusSize = rankMap.size();
        ctx.output("Link graph loaded: " + corpusSize + " pages\n");

        // Step 3: Iterate PageRank
        ctx.output("\nStep 3: Running PageRank iterations...\n");
        final int n = corpusSize;
        double totalChange = Double.MAX_VALUE;
        int iteration = 0;

        while (iteration < MAX_ITERATIONS && totalChange > CONVERGENCE_THRESHOLD_PER_PAGE * n) {
            iteration++;
            long iterStart = System.currentTimeMillis();

            // Calculate contributions in memory
            double danglingSum = 0.0;
            Map<String, Double> contributionMap = new HashMap<>();

            // Initialize all URLs with 0 contribution
            for (String url : rankMap.keySet()) {
                contributionMap.put(url, 0.0);
            }

            // Process each page
            for (Map.Entry<String, Double> entry : rankMap.entrySet()) {
                String url = entry.getKey();
                double rank = entry.getValue();
                String outlinksStr = linkGraphMap.get(url);

                if (outlinksStr == null || outlinksStr.isEmpty()) {
                    // Dangling page - accumulate rank
                    danglingSum += rank;
                } else {
                    // Distribute rank to outlinks
                    String[] outlinks = outlinksStr.split(",");
                    double contribution = rank / outlinks.length;
                    for (String outlink : outlinks) {
                        contributionMap.merge(outlink, contribution, Double::sum);
                    }
                }
            }

            // Calculate dangling contribution per page
            final double danglingContribution = danglingSum / n;
            final double baseRank = (1.0 - DAMPING_FACTOR);

            // Calculate new ranks and convergence
            totalChange = 0.0;
            Map<String, Double> newRankMap = new HashMap<>();

            for (String url : rankMap.keySet()) {
                double oldRank = rankMap.get(url);
                double incomingContrib = contributionMap.getOrDefault(url, 0.0);
                double newRank = baseRank + DAMPING_FACTOR * (incomingContrib + danglingContribution);

                totalChange += Math.abs(newRank - oldRank);
                newRankMap.put(url, newRank);
            }

            rankMap = newRankMap;

            long iterEnd = System.currentTimeMillis();
            ctx.output(String.format("  Iteration %d: change=%.6f, danglingSum=%.2f, time=%dms\n",
                    iteration, totalChange, danglingSum, iterEnd - iterStart));
        }

        // Step 4: Store final ranks to KVS in batches
        ctx.output("\nStep 4: Storing final ranks to " + PAGERANK_TABLE + "...\n");

        List<Row> rowBatch = new ArrayList<>();
        for (Map.Entry<String, Double> entry : rankMap.entrySet()) {
            String url = entry.getKey();
            double rank = entry.getValue();
            String hashedKey = Hasher.hash(url);
            Row row = new Row(hashedKey);
            row.put("url", url);
            row.put("rank", String.valueOf(rank));
            rowBatch.add(row);

            if (rowBatch.size() >= BATCH_SIZE) {
                kvs.putRowBatch(PAGERANK_TABLE, rowBatch, false);
                rowBatch.clear();
            }
        }
        // Write remaining batch
        if (!rowBatch.isEmpty()) {
            kvs.putRowBatch(PAGERANK_TABLE, rowBatch, false);
        }

        long endTime = System.currentTimeMillis();
        int finalCount = kvs.count(PAGERANK_TABLE);

        ctx.output("\n=== PageRank Complete ===\n");
        ctx.output("Total iterations: " + iteration + "\n");
        ctx.output("Final convergence: " + String.format("%.6f", totalChange) + "\n");
        ctx.output("Pages ranked: " + finalCount + "\n");
        ctx.output("Time taken: " + (endTime - startTime) / 1000 + " seconds\n");
    }

    /**
     * Extract links from HTML content, filtering to closed corpus.
     */
    static List<String> extractLinks(String htmlContent, String baseURL, Set<String> corpus) {
        List<String> links = new ArrayList<>();
        Matcher matcher = ANCHOR_PATTERN.matcher(htmlContent);

        while (matcher.find()) {
            String href = matcher.group(1);
            String normalized = normalizeURL(href, baseURL);
            if (normalized != null && corpus.contains(normalized)) {
                links.add(normalized);
            }
        }

        return links;
    }

    /**
     * Normalize a URL relative to a base URL.
     * Reuses logic from Crawler.
     */
    static String normalizeURL(String url, String baseURL) {
        url = url.trim();

        // Remove fragment
        int hashIndex = url.indexOf('#');
        if (hashIndex >= 0) {
            url = url.substring(0, hashIndex);
        }

        // Remove query string
        int queryIndex = url.indexOf('?');
        if (queryIndex >= 0) {
            url = url.substring(0, queryIndex);
        }

        if (url.isEmpty()) {
            return null;
        }

        String[] parts = URLParser.parseURL(url);

        // If URL has protocol and host, it's absolute
        if (parts[0] != null && parts[1] != null) {
            String protocol = parts[0];
            String host = parts[1];
            String port = parts[2];
            String path = parts[3];

            if (port == null) {
                if (protocol.equals("https")) {
                    port = "443";
                } else if (protocol.equals("http")) {
                    port = "80";
                } else {
                    return null;
                }
            }

            if (path == null || path.isEmpty()) {
                path = "/";
            }

            return protocol + "://" + host + ":" + port + path;
        }

        // Relative URL - resolve against base
        String[] baseParts = URLParser.parseURL(baseURL);
        if (baseParts[0] == null || baseParts[1] == null) {
            return null;
        }

        String protocol = baseParts[0];
        String host = baseParts[1];
        String port = baseParts[2];
        String basePath = baseParts[3];

        if (port == null) {
            if (protocol.equals("https")) {
                port = "443";
            } else if (protocol.equals("http")) {
                port = "80";
            } else {
                return null;
            }
        }

        String newPath;
        if (url.startsWith("/")) {
            newPath = url;
        } else {
            int lastSlash = basePath.lastIndexOf('/');
            String baseDir = (lastSlash >= 0) ? basePath.substring(0, lastSlash + 1) : "/";
            newPath = baseDir + url;
        }

        // Resolve .. and .
        newPath = resolvePath(newPath);
        return protocol + "://" + host + ":" + port + newPath;
    }

    /**
     * Resolve . and .. in a path.
     */
    static String resolvePath(String path) {
        String[] parts = path.split("/");
        List<String> resolved = new ArrayList<>();

        for (String part : parts) {
            if (part.equals("..")) {
                if (!resolved.isEmpty()) {
                    resolved.remove(resolved.size() - 1);
                }
            } else if (!part.equals(".") && !part.isEmpty()) {
                resolved.add(part);
            }
        }

        StringBuilder result = new StringBuilder();
        for (String part : resolved) {
            result.append("/").append(part);
        }

        return result.length() == 0 ? "/" : result.toString();
    }
}
