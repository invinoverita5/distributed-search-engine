package cis5550.search;

import java.util.*;
import java.util.regex.*;

import org.tartarus.snowball.ext.englishStemmer;

import cis5550.kvs.*;
import cis5550.tools.*;

public class Ranker {
    static final Logger logger = Logger.getLogger(Ranker.class);

    static final String INDEX_TABLE = "pt-inverted-index";
    static final String PAGERANK_TABLE = "pt-pagerank";
    static final String CRAWL_TABLE = "pt-crawl";

    static final int MAX_RESULTS = 10;
    static final int CANDIDATE_POOL_SIZE = 50; // Fetch PageRank only for top TF-IDF candidates
    static final double TFIDF_WEIGHT = 0.7;
    static final double PAGERANK_WEIGHT = 0.3;

    static final int MIN_WORD_LENGTH = 2;
    static final int MAX_WORD_LENGTH = 50;
    static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z0-9]+");

    // Patterns for stripping non-content HTML sections (with DOTALL to match across newlines)
    static final Pattern SCRIPT_PATTERN = Pattern.compile("<script[^>]*>.*?</script>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern STYLE_PATTERN = Pattern.compile("<style[^>]*>.*?</style>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern NOSCRIPT_PATTERN = Pattern.compile("<noscript[^>]*>.*?</noscript>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern SVG_PATTERN = Pattern.compile("<svg[^>]*>.*?</svg>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern HEAD_PATTERN = Pattern.compile("<head[^>]*>.*?</head>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern COMMENT_PATTERN = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
    static final Pattern TITLE_PATTERN = Pattern.compile("<title[^>]*>([^<]*)</title>", Pattern.CASE_INSENSITIVE);

    private final KVSClient kvs;
    private int totalDocs = -1;

    public Ranker(String kvsCoordinator) {
        this.kvs = new KVSClient(kvsCoordinator);
    }

    public List<SearchResult> search(String query) {
        if (query == null || query.trim().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> terms = tokenize(query);
        if (terms.isEmpty()) {
            return Collections.emptyList();
        }

        // Get total document count for IDF calculation
        int n = getTotalDocs();
        if (n == 0) {
            return Collections.emptyList();
        }

        // Map: url -> accumulated TF-IDF score
        Map<String, Double> tfidfScores = new HashMap<>();

        // For each query term, look up inverted index and calculate TF-IDF
        for (String term : terms) {
            try {
                Row row = kvs.getRow(INDEX_TABLE, term);
                if (row == null) {
                    continue;
                }

                // Document frequency = number of columns (each column is a URL)
                Set<String> urls = row.columns();
                int df = urls.size();
                if (df == 0) {
                    continue;
                }

                // IDF = log(N / df)
                double idf = Math.log((double) n / df);

                // For each document containing this term
                for (String url : urls) {
                    String countStr = row.get(url);
                    if (countStr == null) {
                        continue;
                    }

                    int tf = Integer.parseInt(countStr);
                    // TF-IDF = (1 + log(tf)) * idf (using log-scaled TF)
                    double tfidf = (1 + Math.log(tf)) * idf;

                    tfidfScores.merge(url, tfidf, Double::sum);
                }
            } catch (Exception e) {
                logger.error("Error looking up term: " + term, e);
            }
        }

        if (tfidfScores.isEmpty()) {
            return Collections.emptyList();
        }

        // Normalize TF-IDF scores to [0, 1]
        double maxTfidf = tfidfScores.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0);

        // First pass: sort by TF-IDF alone to get candidate pool
        List<Map.Entry<String, Double>> sortedByTfidf = new ArrayList<>(tfidfScores.entrySet());
        sortedByTfidf.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        // Take top candidates for PageRank lookup (reduces KVS calls from thousands to ~50)
        List<Map.Entry<String, Double>> candidates = sortedByTfidf.subList(
            0, Math.min(CANDIDATE_POOL_SIZE, sortedByTfidf.size())
        );

        // Second pass: fetch PageRank only for candidates and combine scores
        List<SearchResult> results = new ArrayList<>();

        for (Map.Entry<String, Double> entry : candidates) {
            String url = entry.getKey();
            double normalizedTfidf = entry.getValue() / maxTfidf;

            double pagerank = getPageRank(url);
            // Normalize PageRank (typical values are around 1.0, can go higher)
            double normalizedPagerank = Math.min(pagerank / 10.0, 1.0);

            double combinedScore = TFIDF_WEIGHT * normalizedTfidf + PAGERANK_WEIGHT * normalizedPagerank;

            results.add(new SearchResult(url, combinedScore, pagerank));
        }

        // Sort by combined score descending
        results.sort((a, b) -> Double.compare(b.score, a.score));

        // Take top N results
        List<SearchResult> topResults = results.subList(0, Math.min(MAX_RESULTS, results.size()));

        // Fetch snippets and titles for top results
        for (SearchResult result : topResults) {
            populateResultMetadata(result, terms);
        }

        return topResults;
    }

    List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        Matcher matcher = WORD_PATTERN.matcher(text.toLowerCase());
        englishStemmer stemmer = new englishStemmer();

        while (matcher.find()) {
            String word = matcher.group();
            if (word.length() >= MIN_WORD_LENGTH && word.length() <= MAX_WORD_LENGTH) {
                // Stem the word to match indexed terms
                stemmer.setCurrent(word);
                stemmer.stem();
                tokens.add(stemmer.getCurrent());
            }
        }

        return tokens;
    }

    int getTotalDocs() {
        if (totalDocs < 0) {
            try {
                totalDocs = kvs.count(CRAWL_TABLE);
            } catch (Exception e) {
                logger.error("Error getting document count", e);
                totalDocs = 0;
            }
        }
        return totalDocs;
    }

    double getPageRank(String url) {
        try {
            String hashedKey = Hasher.hash(url);
            Row row = kvs.getRow(PAGERANK_TABLE, hashedKey);
            if (row != null) {
                String rankStr = row.get("rank");
                if (rankStr != null) {
                    return Double.parseDouble(rankStr);
                }
            }
        } catch (Exception e) {
            logger.error("Error getting PageRank for: " + url, e);
        }
        return 1.0; // Default PageRank
    }

    /**
     * Fetches crawl data once and populates both snippet and title.
     * Reduces KVS calls from 2 to 1 per result.
     */
    void populateResultMetadata(SearchResult result, List<String> queryTerms) {
        try {
            String hashedKey = Hasher.hash(result.url);
            Row row = kvs.getRow(CRAWL_TABLE, hashedKey);
            if (row == null) {
                return;
            }
            result.snippet = extractSnippetFromRow(row, queryTerms);
            result.title = extractTitleFromRow(row, result.url);
        } catch (Exception e) {
            logger.error("Error fetching metadata for: " + result.url, e);
        }
    }

    String getSnippet(String url, List<String> queryTerms) {
        try {
            String hashedKey = Hasher.hash(url);
            Row row = kvs.getRow(CRAWL_TABLE, hashedKey);
            if (row == null) {
                return "";
            }

            // Use pre-extracted plain text if available, fall back to parsing content
            String text = row.get("text");
            if (text == null || text.isEmpty()) {
                // Fallback: parse from raw HTML content
                text = extractTextFromHtml(row.get("content"));
            }
            if (text == null || text.isEmpty()) {
                return "";
            }

            // Find a snippet containing query terms
            String lowerContent = text.toLowerCase();
            int bestPos = -1;
            int bestScore = 0;

            for (String term : queryTerms) {
                int pos = lowerContent.indexOf(term);
                if (pos >= 0) {
                    // Count how many query terms appear near this position
                    int score = 0;
                    for (String t : queryTerms) {
                        int start = Math.max(0, pos - 100);
                        int end = Math.min(lowerContent.length(), pos + 200);
                        if (lowerContent.substring(start, end).contains(t)) {
                            score++;
                        }
                    }
                    if (score > bestScore) {
                        bestScore = score;
                        bestPos = pos;
                    }
                }
            }

            if (bestPos < 0) {
                bestPos = 0;
            }

            // Extract snippet around best position
            int start = Math.max(0, bestPos - 50);
            int end = Math.min(text.length(), bestPos + 200);

            // Adjust to word boundaries
            if (start > 0) {
                int spacePos = text.indexOf(' ', start);
                if (spacePos > 0 && spacePos < start + 20) {
                    start = spacePos + 1;
                }
            }
            if (end < text.length()) {
                int spacePos = text.lastIndexOf(' ', end);
                if (spacePos > end - 20) {
                    end = spacePos;
                }
            }

            String snippet = text.substring(start, end);
            if (start > 0) {
                snippet = "..." + snippet;
            }
            if (end < text.length()) {
                snippet = snippet + "...";
            }

            return snippet;
        } catch (Exception e) {
            logger.error("Error getting snippet for: " + url, e);
            return "";
        }
    }

    String extractTitle(String url) {
        try {
            String hashedKey = Hasher.hash(url);
            Row row = kvs.getRow(CRAWL_TABLE, hashedKey);
            if (row == null) {
                return url;
            }

            // Use pre-extracted title if available
            String title = row.get("title");
            if (title != null && !title.isEmpty() && !title.equals(url)) {
                return title;
            }

            // Fallback: extract from raw HTML content
            String content = row.get("content");
            if (content == null) {
                return url;
            }

            // Unescape HTML entities (loop to handle double/triple encoding)
            String prev;
            do {
                prev = content;
                content = org.apache.commons.text.StringEscapeUtils.unescapeHtml4(content);
            } while (!content.equals(prev));

            // Extract title from <title> tag
            Pattern titlePattern = Pattern.compile("<title[^>]*>([^<]*)</title>", Pattern.CASE_INSENSITIVE);
            Matcher matcher = titlePattern.matcher(content);
            if (matcher.find()) {
                String extractedTitle = matcher.group(1).trim();
                if (!extractedTitle.isEmpty()) {
                    return extractedTitle;
                }
            }

            return url;
        } catch (Exception e) {
            return url;
        }
    }

    /**
     * Extract snippet from a pre-fetched Row (avoids redundant KVS call).
     */
    String extractSnippetFromRow(Row row, List<String> queryTerms) {
        // Use pre-extracted plain text if available, fall back to parsing content
        String text = row.get("text");
        if (text == null || text.isEmpty()) {
            text = extractTextFromHtml(row.get("content"));
        }
        if (text == null || text.isEmpty()) {
            return "";
        }

        // Find a snippet containing query terms
        String lowerContent = text.toLowerCase();
        int bestPos = -1;
        int bestScore = 0;

        for (String term : queryTerms) {
            int pos = lowerContent.indexOf(term);
            if (pos >= 0) {
                int score = 0;
                for (String t : queryTerms) {
                    int start = Math.max(0, pos - 100);
                    int end = Math.min(lowerContent.length(), pos + 200);
                    if (lowerContent.substring(start, end).contains(t)) {
                        score++;
                    }
                }
                if (score > bestScore) {
                    bestScore = score;
                    bestPos = pos;
                }
            }
        }

        if (bestPos < 0) {
            bestPos = 0;
        }

        // Extract snippet around best position
        int start = Math.max(0, bestPos - 50);
        int end = Math.min(text.length(), bestPos + 200);

        // Adjust to word boundaries
        if (start > 0) {
            int spacePos = text.indexOf(' ', start);
            if (spacePos > 0 && spacePos < start + 20) {
                start = spacePos + 1;
            }
        }
        if (end < text.length()) {
            int spacePos = text.lastIndexOf(' ', end);
            if (spacePos > end - 20) {
                end = spacePos;
            }
        }

        String snippet = text.substring(start, end);
        if (start > 0) {
            snippet = "..." + snippet;
        }
        if (end < text.length()) {
            snippet = snippet + "...";
        }

        return snippet;
    }

    /**
     * Extract title from a pre-fetched Row (avoids redundant KVS call).
     * Uses static TITLE_PATTERN instead of compiling per call.
     */
    String extractTitleFromRow(Row row, String url) {
        // Use pre-extracted title if available
        String title = row.get("title");
        if (title != null && !title.isEmpty() && !title.equals(url)) {
            return title;
        }

        // Fallback: extract from raw HTML content
        String content = row.get("content");
        if (content == null) {
            return url;
        }

        // Unescape HTML entities (loop to handle double/triple encoding)
        String prev;
        do {
            prev = content;
            content = org.apache.commons.text.StringEscapeUtils.unescapeHtml4(content);
        } while (!content.equals(prev));

        // Extract title from <title> tag using static pattern
        Matcher matcher = TITLE_PATTERN.matcher(content);
        if (matcher.find()) {
            String extractedTitle = matcher.group(1).trim();
            if (!extractedTitle.isEmpty()) {
                return extractedTitle;
            }
        }

        return url;
    }

    /**
     * Fallback method to extract plain text from HTML content.
     * Used when pre-extracted text is not available.
     */
    String extractTextFromHtml(String content) {
        if (content == null) {
            return null;
        }

        // Unescape HTML entities (loop to handle double/triple encoding)
        String prev;
        do {
            prev = content;
            content = org.apache.commons.text.StringEscapeUtils.unescapeHtml4(content);
        } while (!content.equals(prev));

        // Strip non-content HTML sections
        content = SCRIPT_PATTERN.matcher(content).replaceAll(" ");
        content = STYLE_PATTERN.matcher(content).replaceAll(" ");
        content = NOSCRIPT_PATTERN.matcher(content).replaceAll(" ");
        content = SVG_PATTERN.matcher(content).replaceAll(" ");
        content = HEAD_PATTERN.matcher(content).replaceAll(" ");
        content = COMMENT_PATTERN.matcher(content).replaceAll(" ");
        // Strip remaining HTML tags
        content = content.replaceAll("<[^>]+>", " ");
        content = content.replaceAll("\\s+", " ").trim();

        return content;
    }

    public static class SearchResult {
        public String url;
        public double score;
        public double pagerank;
        public String title;
        public String snippet;

        public SearchResult(String url, double score, double pagerank) {
            this.url = url;
            this.score = score;
            this.pagerank = pagerank;
            this.title = url;
            this.snippet = "";
        }

        public String toJson() {
            // Simple JSON serialization
            return String.format(
                "{\"url\":\"%s\",\"title\":\"%s\",\"snippet\":\"%s\",\"score\":%.4f}",
                escapeJson(url),
                escapeJson(title),
                escapeJson(snippet),
                score
            );
        }

        private String escapeJson(String s) {
            if (s == null) return "";
            return s.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }
    }

    public static String resultsToJson(List<SearchResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"results\":[");
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(results.get(i).toJson());
        }
        sb.append("]}");
        return sb.toString();
    }
}
