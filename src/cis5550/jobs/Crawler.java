package cis5550.jobs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;

import java.util.regex.*;

import com.google.common.net.UrlEscapers;
import org.apache.commons.text.StringEscapeUtils;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;

public class Crawler {
    static final Logger logger = Logger.getLogger(Crawler.class);

    static final String USER_AGENT = "cis5550-crawler";

    static final String DOMAINS_TABLE = "pt-domains";
    static final String PAGES_TABLE = "pt-pages";
    static final String FRONTIER_TABLE = "pt-frontier";
    public static final String CRAWL_TABLE = "pt-crawl";

    static final int DEFAULT_MAX_PAGES = 5000;
    static final int MAX_PAGE_SIZE = 10 * 1024 * 1024; // 10 MB
    static final int BATCH_SIZE = 250;
    static final int DEFAULT_CONNECT_TIMEOUT = 5 * 1000; // 5 seconds
    static final int DEFAULT_READ_TIMEOUT = 15 * 1000; // 15 seconds
    static final int TASK_TIMEOUT = 10 * 60 * 1000; // 10 minutes

    public static void run(FlameContext context, String[] args) throws Exception {
        KVSClient kvs = context.getKVS();

        // parse max pages from first argument
        int maxPages = DEFAULT_MAX_PAGES;
        String[] seedUrls = args;
        if (args.length > 0) {
            try {
                maxPages = Integer.parseInt(args[0]);
                seedUrls = Arrays.copyOfRange(args, 1, args.length);
            } catch (NumberFormatException e) {
                // first arg is not a number, treat all args as seed URLs
            }
        }

        // load the URL frontier
        FlameRDD frontier;
        if (kvs.count(FRONTIER_TABLE) > 0) {
            frontier = context.fromTable(FRONTIER_TABLE, row -> row.get("value"));
        } else if (seedUrls.length > 0) {
            frontier = context.parallelize(new ArrayList<>(List.of(seedUrls)));
            frontier.saveAsTable(FRONTIER_TABLE);
        } else {
            logger.error("Missing seed URLs, either load as arguments or into pt-frontier table.");
            System.exit(1);
            return;
        }

        final String kvsCoordinator = context.getKVS().getCoordinator();
        int initialCount = kvs.count(CRAWL_TABLE);
        int currentCount = initialCount;
        long startTimeStamp = System.currentTimeMillis();
        int batchNumber = 1;

        context.output("\n");
        while (frontier.count() > 0 && currentCount < maxPages) {
            long batchStartTime = System.currentTimeMillis();
            Vector<String> batch = frontier.take(BATCH_SIZE);
            frontier.destroy();

            FlameRDD frontierBatch = context.parallelize(new ArrayList<>(batch));
            frontierBatch.saveAsTable(FRONTIER_TABLE);

            int beforeCrawlCount = kvs.count(CRAWL_TABLE);
            FlameRDD extractedURLs = frontierBatch.mapPartitions(urlIterator -> {
                // Collect all URLs from the partition
                List<String> urls = new ArrayList<>();
                while (urlIterator.hasNext()) {
                    urls.add(urlIterator.next());
                }

                // Create a single shared KVSClient for all virtual threads in this partition
                KVSClient sharedKvs = new KVSClient(kvsCoordinator);
                // Pre-initialize the worker list to avoid synchronized block contention in virtual threads
                sharedKvs.numWorkers();

                // Local cache for domain metadata to avoid redundant KVS/HTTP calls for same domain
                ConcurrentHashMap<String, DomainMetadata> domainCache = new ConcurrentHashMap<>();

                // Limit concurrent crawls to control memory usage (socket buffers, page content)
                Semaphore crawlPermits = new Semaphore(25);

                // Process URLs concurrently using virtual threads
                List<String> allExtractedUrls = Collections.synchronizedList(new ArrayList<>());
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    List<Future<?>> futures = new ArrayList<>();
                    for (String pageUrl : urls) {
                        futures.add(executor.submit(() -> {
                            try {
                                crawlPermits.acquire();
                                try {
                                    List<String> extracted = crawlSingleUrl(pageUrl, sharedKvs, domainCache);
                                    if (extracted != null) {
                                        allExtractedUrls.addAll(extracted);
                                    }
                                } finally {
                                    crawlPermits.release();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }));
                    }
                    // Wait for all tasks to complete
                    for (Future<?> f : futures) {
                        try {
                            f.get(TASK_TIMEOUT, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            f.cancel(true);
                        } catch (Exception e) {
                            // Individual URL failures logged in crawlSingleUrl
                        }
                    }
                }

                return allExtractedUrls.iterator();
            });

            FlameRDD distinctURLs = extractedURLs.distinct();
            FlameRDD crawledURLs = context.fromTable(CRAWL_TABLE, row -> row.get("url"));
            frontier = distinctURLs.subtract(crawledURLs.getTableName());

            // garbage collection
            extractedURLs.destroy();
            distinctURLs.destroy();
            crawledURLs.destroy();

            // diagnostic information
            int afterCrawlCount = kvs.count(CRAWL_TABLE);
            int pagesCrawledInBatch = afterCrawlCount - beforeCrawlCount;
            int newUrlsDiscovered = frontier.count();
            long batchEndTime = System.currentTimeMillis();
            long batchDuration = batchEndTime - batchStartTime;

            context.output(String.format("Batch %d: %d pages crawled in %.2f seconds (%.2f pages/sec), " +
                    "%d new URLs discovered, total crawled: %d/%d\n",
                    batchNumber,
                    pagesCrawledInBatch,
                    batchDuration / 1000.0,
                    pagesCrawledInBatch / (batchDuration / 1000.0),
                    newUrlsDiscovered,
                    afterCrawlCount,
                    maxPages));

            currentCount = afterCrawlCount;
            ++batchNumber;
        }
        context.output("\n");
        frontier.saveAsTable(FRONTIER_TABLE);

        long endTimeStamp = System.currentTimeMillis();
        int finalCount = kvs.count(CRAWL_TABLE);
        context.output("Finished crawling " + (finalCount - initialCount) + " pages in "
                + (endTimeStamp - startTimeStamp) / 1000 + " seconds!");
    }

    /**
     * Crawl a single URL and return extracted URLs
     */
    static List<String> crawlSingleUrl(String pageUrl, KVSClient kvs, ConcurrentHashMap<String, DomainMetadata> domainCache) {
        try {
            logger.debug("crawlSingleUrl: START " + pageUrl);
            String[] parts = URLParser.parseURL(pageUrl);
            String baseUrl = URLParser.getBaseURL(parts);
            if (baseUrl == null) {
                logger.debug("crawlSingleUrl: SKIP (no baseUrl) " + pageUrl);
                return null;
            }

            String path = parts[3];

            logger.debug("crawlSingleUrl: fetching domain metadata for " + baseUrl);
            DomainMetadata domainMetadata = getOrDownloadDomainMetadata(baseUrl, kvs, domainCache);
            if (!shouldCrawlDomain(domainMetadata, path)) {
                logger.debug("crawlSingleUrl: SKIP (domain rules) " + pageUrl);
                return null;
            }

            logger.debug("crawlSingleUrl: fetching page metadata for " + pageUrl);
            PageMetadata pageMetadata = getOrDownloadPageMetadata(pageUrl, kvs);
            if (!shouldCrawlPage(pageMetadata)) {
                logger.debug("crawlSingleUrl: SKIP (page rules) " + pageUrl);
                return null;
            }

            logger.debug("crawlSingleUrl: downloading page " + pageUrl);
            String content = downloadPage(pageUrl, kvs);
            if (content != null) {
                logger.debug("crawlSingleUrl: SUCCESS " + pageUrl);
                return extractURLs(content, pageUrl);
            }

            logger.debug("crawlSingleUrl: FAIL (no content) " + pageUrl);
            return null;
        } catch (Exception e) {
            logger.debug("crawlSingleUrl: ERROR " + pageUrl + " - " + e.getMessage());
            return null;
        }
    }

    /**
     * HTTP helper methods
     */
    static HttpURLConnection createConnection(String urlString, String method) throws IOException {
        return createConnection(urlString, method, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT);
    }

    static HttpURLConnection createConnection(String urlString, String method, int connectTimeout,
            int readTimeout) throws IOException {
        URL url = encodeUrl(urlString).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setInstanceFollowRedirects(false);
        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);
        return conn;
    }

    static URI encodeUrl(String urlString) throws IOException {
        int schemeEnd = urlString.indexOf("://");
        if (schemeEnd == -1)
            throw new IOException("Invalid URL: " + urlString);

        int pathStart = urlString.indexOf('/', schemeEnd + 3);
        if (pathStart == -1) {
            return URI.create(urlString);
        }

        String base = urlString.substring(0, pathStart);
        String pathAndRest = urlString.substring(pathStart);

        StringBuilder encoded = new StringBuilder();
        for (String segment : pathAndRest.split("/", -1)) {
            if (encoded.length() > 0)
                encoded.append("/");
            encoded.append(UrlEscapers.urlPathSegmentEscaper().escape(segment));
        }

        // Ensure path starts with / (first segment is empty when path starts with /)
        if (pathAndRest.startsWith("/") && !encoded.toString().startsWith("/")) {
            encoded.insert(0, "/");
        }

        return URI.create(base + encoded);
    }

    static byte[] readBytes(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(data)) != -1) {
            buffer.write(data, 0, bytesRead);
        }
        return buffer.toByteArray();
    }

    /**
     * domain-level analysis
     */
    static class DomainMetadata {
        String baseUrl;
        long lastCrawlTime = -1;
        long lastFetchedTime = -1;
        String robots = null;

        DomainMetadata(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        DomainMetadata(String baseUrl, long lastFetchedTime, String robots) {
            this.baseUrl = baseUrl;
            this.lastFetchedTime = lastFetchedTime;
            this.robots = robots;
        }

        static DomainMetadata fromRow(Row row) {
            DomainMetadata metadata = new DomainMetadata(row.key());
            String lastCrawlTime = row.get("lastCrawlTime");
            if (lastCrawlTime != null) {
                metadata.lastCrawlTime = Long.parseLong(lastCrawlTime);
            }
            String lastFetchedTime = row.get("lastFetchedTime");
            if (lastFetchedTime != null) {
                metadata.lastFetchedTime = Long.parseLong(lastFetchedTime);
            }
            metadata.robots = row.get("robots");
            return metadata;
        }

        Row toRow() {
            String hashedKey = Hasher.hash(baseUrl);
            Row row = new Row(hashedKey);
            row.put("url", baseUrl);
            if (lastCrawlTime != -1) {
                row.put("lastCrawlTime", String.valueOf(lastCrawlTime));
            }
            if (lastFetchedTime != -1) {
                row.put("lastFetchedTime", String.valueOf(lastFetchedTime));
            }
            if (robots != null) {
                row.put("robots", robots);
            }
            return row;
        }
    }

    static class RobotsTxt {
        List<String> allowRules = new ArrayList<>();
        List<String> disallowRules = new ArrayList<>();
        double crawlDelay = 0.0;

        public boolean isAllowed(String path) {
            int allowRuleLength = 0;
            int disallowRuleLength = 0;

            for (String allowRule : allowRules) {
                if (path.startsWith(allowRule)) {
                    allowRuleLength = Math.max(allowRuleLength, allowRule.length());
                }
            }

            for (String disallowRule : disallowRules) {
                if (path.startsWith(disallowRule)) {
                    disallowRuleLength = Math.max(disallowRuleLength, disallowRule.length());
                }
            }

            // if no rules match, 0 >= 0 so returns true by default
            return allowRuleLength >= disallowRuleLength;
        }

        public void addAllowRule(String allowRule) {
            allowRules.add(allowRule);
        }

        public void addDisallowRule(String disallowRule) {
            disallowRules.add(disallowRule);
        }

        public void setCrawlDelay(double seconds) {
            crawlDelay = seconds;
        }
    }

    static DomainMetadata getOrDownloadDomainMetadata(String baseUrl, KVSClient kvs, ConcurrentHashMap<String, DomainMetadata> domainCache) {
        // Check local in-memory cache first (avoids redundant KVS/HTTP calls for same domain in batch)
        DomainMetadata localCached = domainCache.get(baseUrl);
        if (localCached != null) {
            logger.debug("getOrDownloadDomainMetadata: using local cache for " + baseUrl);
            return localCached;
        }

        try {
            String hashedKey = Hasher.hash(baseUrl);
            logger.debug("getOrDownloadDomainMetadata: checking KVS cache for " + baseUrl);
            Row row = kvs.getRow(DOMAINS_TABLE, hashedKey);
            logger.debug("getOrDownloadDomainMetadata: KVS lookup complete, row=" + (row != null));
            if (row != null) {
                DomainMetadata cached = DomainMetadata.fromRow(row);
                boolean isFresh = cached.lastFetchedTime != -1 &&
                        (System.currentTimeMillis() - cached.lastFetchedTime) < 30L * 24 * 60 * 60 * 1000;
                if (isFresh && cached.robots != null) {
                    logger.debug("getOrDownloadDomainMetadata: using KVS cached data for " + baseUrl);
                    domainCache.put(baseUrl, cached);
                    return cached;
                }
            }
            logger.debug("getOrDownloadDomainMetadata: downloading fresh metadata for " + baseUrl);
            DomainMetadata metadata = downloadDomainMetadata(baseUrl);
            if (metadata != null) {
                logger.debug("getOrDownloadDomainMetadata: storing metadata in KVS for " + baseUrl);
                kvs.putRow(DOMAINS_TABLE, metadata.toRow());
                domainCache.put(baseUrl, metadata);
            }
            logger.debug("getOrDownloadDomainMetadata: done for " + baseUrl);
            return metadata;
        } catch (IOException e) {
            logger.debug("getOrDownloadDomainMetadata: ERROR for " + baseUrl + " - " + e.getMessage());
            return null;
        }
    }

    static DomainMetadata downloadDomainMetadata(String baseUrl) {
        String robotsTxt = downloadRobotsTxt(baseUrl);
        long lastFetchedTime = System.currentTimeMillis();
        return new DomainMetadata(baseUrl, lastFetchedTime, robotsTxt);
    }

    static String getOrDownloadRobotsTxt(String baseUrl, KVSClient kvs) {
        try {
            String hashedKey = Hasher.hash(baseUrl);
            Row row = kvs.getRow(DOMAINS_TABLE, hashedKey);
            String robotsTxt = row.get("robots");
            if (robotsTxt != null) {
                return robotsTxt;
            }
            robotsTxt = downloadRobotsTxt(baseUrl);
            if (robotsTxt != null) {
                kvs.put(DOMAINS_TABLE, hashedKey, "robots", robotsTxt);
            }
            return robotsTxt;
        } catch (IOException e) {
            return null;
        }
    }

    static String downloadRobotsTxt(String baseUrl) {
        HttpURLConnection conn = null;
        try {
            logger.debug("downloadRobotsTxt: creating connection to " + baseUrl + "/robots.txt");
            conn = createConnection(baseUrl + "/robots.txt", "GET");
            logger.debug("downloadRobotsTxt: connecting...");
            conn.connect();
            logger.debug("downloadRobotsTxt: connected, getting response code...");

            int responseCode = conn.getResponseCode();
            logger.debug("downloadRobotsTxt: response code " + responseCode);
            if (responseCode != 200) {
                return null;
            }

            logger.debug("downloadRobotsTxt: reading response body...");
            String result = new String(readBytes(conn.getInputStream()));
            logger.debug("downloadRobotsTxt: SUCCESS (" + result.length() + " bytes)");
            return result;
        } catch (IOException e) {
            logger.debug("downloadRobotsTxt: FAILED - " + e.getMessage());
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    static RobotsTxt parseRobotsTxt(String robotsTxtContent) {
        RobotsTxt rules = new RobotsTxt();
        if (robotsTxtContent == null) {
            return rules;
        }

        String[] lines = robotsTxtContent.split("\n");
        boolean foundSpecificSection = false;
        boolean parsingSection = false;

        for (String line : lines) {
            int commentIdx = line.indexOf("#");
            if (commentIdx != -1) {
                line = line.substring(0, commentIdx);
            }
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            int colonIdx = line.indexOf(":");
            if (colonIdx == -1) {
                continue;
            }

            String directive = line.substring(0, colonIdx).trim().toLowerCase();
            String value = line.substring(colonIdx + 1).trim();

            if (directive.equals("user-agent")) {
                if (foundSpecificSection) {
                    break;
                }
                if (value.equalsIgnoreCase(USER_AGENT)) {
                    foundSpecificSection = true;
                    parsingSection = true;
                    rules = new RobotsTxt();
                } else {
                    parsingSection = value.equals("*");
                }
            } else if (parsingSection) {
                if (directive.equals("allow")) {
                    rules.addAllowRule(value);
                } else if (directive.equals("disallow")) {
                    rules.addDisallowRule(value);
                } else if (directive.equals("crawl-delay")) {
                    try {
                        rules.setCrawlDelay(Double.parseDouble(value));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        return rules;
    }

    static boolean shouldCrawlDomain(DomainMetadata domainMetadata, String path) {
        if (domainMetadata == null) {
            return false;
        }
        RobotsTxt rules = parseRobotsTxt(domainMetadata.robots);
        if (!rules.isAllowed(path)) {
            return false;
        }
        if (rules.crawlDelay > 0 && domainMetadata.lastCrawlTime != -1) {
            long timeSinceLastCrawl = System.currentTimeMillis() - domainMetadata.lastCrawlTime;
            if (timeSinceLastCrawl < rules.crawlDelay * 1000) {
                return false;
            }
        }
        return true;
    }

    /**
     * page-level analysis
     */
    static class PageMetadata {
        String pageUrl;
        String contentType = null;
        String contentLanguage = null;

        int lastStatusCode = -1;
        int contentLength = -1;

        long lastFetchedTime = -1;

        PageMetadata(String pageUrl) {
            this.pageUrl = pageUrl;
        }

        PageMetadata(String pageUrl, int lastStatusCode, String contentType, String contentLength,
                String contentLanguage, long lastFetchedTime) {
            this.pageUrl = pageUrl;
            this.lastStatusCode = lastStatusCode;
            this.contentType = contentType;
            if (contentLength != null) {
                this.contentLength = Integer.parseInt(contentLength);
            }
            this.contentLanguage = contentLanguage;
            this.lastFetchedTime = lastFetchedTime;
        }

        static PageMetadata fromRow(Row row) {
            PageMetadata metadata = new PageMetadata(row.key());
            metadata.contentType = row.get("contentType");
            metadata.contentLanguage = row.get("contentLanguage");
            String lastStatusCode = row.get("lastStatusCode");
            if (lastStatusCode != null) {
                metadata.lastStatusCode = Integer.parseInt(lastStatusCode);
            }
            String contentLength = row.get("contentLength");
            if (contentLength != null) {
                metadata.contentLength = Integer.parseInt(contentLength);
            }
            String lastFetchedTime = row.get("lastFetchedTime");
            if (lastFetchedTime != null) {
                metadata.lastFetchedTime = Long.parseLong(lastFetchedTime);
            }
            return metadata;
        }

        Row toRow() {
            String hashedKey = Hasher.hash(pageUrl);
            Row row = new Row(hashedKey);
            row.put("url", pageUrl);
            if (lastStatusCode != -1) {
                row.put("lastStatusCode", String.valueOf(lastStatusCode));
            }
            if (contentType != null) {
                row.put("contentType", contentType);
            }
            if (contentLanguage != null) {
                row.put("contentLanguage", contentLanguage);
            }
            if (contentLength != -1) {
                row.put("contentLength", String.valueOf(contentLength));
            }
            if (lastFetchedTime != -1) {
                row.put("lastFetchedTime", String.valueOf(lastFetchedTime));
            }
            return row;
        }
    }

    static PageMetadata getOrDownloadPageMetadata(String pageUrl, KVSClient kvs) {
        try {
            String hashedKey = Hasher.hash(pageUrl);
            Row row = kvs.getRow(PAGES_TABLE, hashedKey);
            if (row != null) {
                PageMetadata cached = PageMetadata.fromRow(row);
                boolean isComplete = cached.lastStatusCode != -1 && cached.contentType != null;
                boolean isFresh = cached.lastFetchedTime != -1 &&
                        (System.currentTimeMillis() - cached.lastFetchedTime) < 7 * 24 * 60 * 60 * 1000;
                if (isComplete && isFresh) {
                    return cached;
                }
            }
            PageMetadata metadata = downloadPageMetadata(pageUrl);
            if (metadata != null) {
                kvs.putRow(PAGES_TABLE, metadata.toRow());
            }
            return metadata;
        } catch (IOException e) {
            return null;
        }
    }

    static PageMetadata downloadPageMetadata(String pageUrl) {
        HttpURLConnection conn = null;
        try {
            logger.debug("downloadPageMetadata: creating HEAD connection to " + pageUrl);
            conn = createConnection(pageUrl, "HEAD");
            logger.debug("downloadPageMetadata: connecting...");
            conn.connect();
            logger.debug("downloadPageMetadata: connected, getting response...");
            long lastFetchedTime = System.currentTimeMillis();
            int responseCode = conn.getResponseCode();
            logger.debug("downloadPageMetadata: response code " + responseCode);
            return new PageMetadata(
                    pageUrl,
                    responseCode,
                    conn.getHeaderField("Content-Type"),
                    conn.getHeaderField("Content-Length"),
                    conn.getHeaderField("Content-Language"),
                    lastFetchedTime);
        } catch (IOException e) {
            logger.debug("downloadPageMetadata: FAILED - " + e.getMessage());
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    static boolean shouldCrawlPage(PageMetadata metadata) {
        if (metadata == null) {
            return false;
        }
        if (metadata.lastStatusCode != 200) {
            return false;
        }
        if (metadata.contentType == null || !metadata.contentType.startsWith("text/html")) {
            return false;
        }
        if (metadata.contentLength != -1 && metadata.contentLength > MAX_PAGE_SIZE) {
            return false;
        }
        // reject non-English pages if Content-Language header is present
        if (metadata.contentLanguage != null && !metadata.contentLanguage.toLowerCase().startsWith("en")) {
            return false;
        }
        return true;
    }

    static String downloadPage(String pageUrl, KVSClient kvs) {
        HttpURLConnection conn = null;
        try {
            logger.debug("downloadPage: creating GET connection to " + pageUrl);
            conn = createConnection(pageUrl, "GET");
            logger.debug("downloadPage: connecting...");
            conn.connect();
            logger.debug("downloadPage: connected, updating lastCrawlTime...");
            String baseUrl = URLParser.getBaseURL(URLParser.parseURL(pageUrl));
            String hashedKey = Hasher.hash(baseUrl);
            kvs.put(DOMAINS_TABLE, hashedKey, "lastCrawlTime", String.valueOf(System.currentTimeMillis()));
            logger.debug("downloadPage: getting response code...");
            int responseCode = conn.getResponseCode();
            logger.debug("downloadPage: response code " + responseCode);
            if (responseCode != 200) {
                return null;
            }
            logger.debug("downloadPage: reading response body...");
            String content = new String(readBytes(conn.getInputStream()));
            logger.debug("downloadPage: read " + content.length() + " bytes, checking language...");
            if (!isEnglishContent(content)) {
                logger.debug("downloadPage: SKIP (not English)");
                return null;
            }
            logger.debug("downloadPage: storing page...");
            storePage(pageUrl, content, kvs);
            logger.debug("downloadPage: SUCCESS");
            return content;
        } catch (IOException e) {
            logger.debug("downloadPage: FAILED - " + e.getMessage());
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    static void storePage(String pageUrl, String content, KVSClient kvs) {
        try {
            String hashedKey = Hasher.hash(pageUrl);
            Row row = new Row(hashedKey);
            row.put("url", pageUrl);
            row.put("content", StringEscapeUtils.escapeHtml4(content));
            kvs.putRow(CRAWL_TABLE, row);
        } catch (IOException e) {
        }
    }

    static final Pattern ANCHOR_PATTERN = Pattern.compile(
            "<a\\s+[^>]*href\\s*=\\s*[\"']([^\"']*)[\"'][^>]*>",
            Pattern.CASE_INSENSITIVE);

    static final Set<String> EXCLUDED_EXTENSIONS = Set.of(
            // images
            "jpg", "jpeg", "png", "gif", "bmp", "svg", "webp", "ico", "tiff",
            // documents
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
            // media
            "mp3", "mp4", "avi", "mov", "wmv", "flv", "wav", "webm",
            // archives
            "zip", "tar", "gz", "rar", "7z",
            // code/data
            "css", "js", "json", "xml", "rss", "atom",
            // other
            "exe", "dmg", "apk", "iso");

    static final Pattern HTML_LANG_PATTERN = Pattern.compile(
            "<html[^>]*\\slang\\s*=\\s*[\"']([^\"']*)[\"']",
            Pattern.CASE_INSENSITIVE);

    static boolean isEnglishContent(String content) {
        Matcher matcher = HTML_LANG_PATTERN.matcher(content);
        if (matcher.find()) {
            String lang = matcher.group(1).toLowerCase();
            return lang.startsWith("en");
        }
        // no lang attribute found - allow by default
        return true;
    }

    static List<String> extractURLs(String htmlContent, String baseURL) {
        List<String> urls = new ArrayList<>();
        Matcher matcher = ANCHOR_PATTERN.matcher(htmlContent);
        while (matcher.find()) {
            String href = matcher.group(1);
            String normalized = normalizeURL(href, baseURL);
            if (normalized != null && isCrawlableURL(normalized)) {
                urls.add(normalized);
            }
        }
        return urls;
    }

    static String normalizeURL(String url, String baseURL) {
        url = url.trim();
        int hashIndex = url.indexOf('#');
        if (hashIndex >= 0) {
            url = url.substring(0, hashIndex);
        }
        int queryIndex = url.indexOf('?');
        if (queryIndex >= 0) {
            url = url.substring(0, queryIndex);
        }
        if (url.isEmpty()) {
            return null;
        }

        String[] parts = URLParser.parseURL(url);

        // if URL has protocol and host, it's absolute
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

        // relative URL - resolve against base
        String[] baseParts = URLParser.parseURL(baseURL);
        if (baseParts[0] == null || baseParts[1] == null) {
            return null;
        }

        String protocol = baseParts[0];
        String host = baseParts[1];
        String port = baseParts[2];
        String basePath = baseParts[3];

        // add default port if missing
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

        // resolve .. and .
        newPath = resolvePath(newPath);
        return protocol + "://" + host + ":" + port + newPath;
    }

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

    /**
     * URL filtering
     */
    static boolean isCrawlableURL(String url) {
        String[] parts = URLParser.parseURL(url);
        String path = parts[3];

        if (hasExcludedExtension(path)) {
            return false;
        }

        return true;
    }

    static boolean hasExcludedExtension(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }

        int lastSlash = path.lastIndexOf('/');
        String segment = (lastSlash >= 0) ? path.substring(lastSlash + 1) : path;

        int dotIndex = segment.lastIndexOf('.');
        if (dotIndex < 0 || dotIndex == segment.length() - 1) {
            return false;
        }

        String extension = segment.substring(dotIndex + 1).toLowerCase();
        return EXCLUDED_EXTENSIONS.contains(extension);
    }
}
