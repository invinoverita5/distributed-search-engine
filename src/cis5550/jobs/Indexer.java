package cis5550.jobs;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

import org.apache.commons.text.StringEscapeUtils;
import org.tartarus.snowball.ext.englishStemmer;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;

public class Indexer {
    static final Logger logger = Logger.getLogger(Indexer.class);

    static final String CRAWL_TABLE = Crawler.CRAWL_TABLE;
    static final String INDEX_TABLE = "pt-inverted-index";

    static final int BATCH_SIZE = 1000;

    // Pattern to match HTML tags
    static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]+>");
    // Pattern to match words (alphabetic only, no numbers)
    static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");

    // Pre-compiled patterns for stripping non-content HTML sections
    static final Pattern SCRIPT_PATTERN = Pattern.compile("<script[^>]*>.*?</script>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern STYLE_PATTERN = Pattern.compile("<style[^>]*>.*?</style>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern NOSCRIPT_PATTERN = Pattern.compile("<noscript[^>]*>.*?</noscript>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern SVG_PATTERN = Pattern.compile("<svg[^>]*>.*?</svg>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern HEAD_PATTERN = Pattern.compile("<head[^>]*>.*?</head>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    static final Pattern COMMENT_PATTERN = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
    static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    static final Pattern TITLE_PATTERN = Pattern.compile("<title[^>]*>([^<]*)</title>", Pattern.CASE_INSENSITIVE);

    // Word length limits for tokenization
    static final int MIN_WORD_LENGTH = 2;
    static final int MAX_WORD_LENGTH = 50;

    // Path to English dictionary file (must be deployed to all workers)
    static final String DICTIONARY_PATH = "words.txt";

    public static void run(FlameContext context, String[] args) throws Exception {
        KVSClient kvs = context.getKVS();
        long startTime = System.currentTimeMillis();

        int pageCount = kvs.count(CRAWL_TABLE);
        context.output("Starting indexer with " + pageCount + " pages from " + CRAWL_TABLE + "\n");

        // Delete existing index table if it exists
        if (kvs.count(INDEX_TABLE) > 0) {
            context.output("Deleting existing " + INDEX_TABLE + " table...\n");
            kvs.delete(INDEX_TABLE);
        }

        final String kvsCoordinator = kvs.getCoordinator();

        // Read crawl data - only pass row keys through RDD to avoid OOM
        // Workers will fetch content directly from KVS
        FlameRDD crawlData = context.fromTable(CRAWL_TABLE, row -> {
            String url = row.get("url");
            String content = row.get("content");
            if (url == null || content == null) {
                return null;
            }
            // Only return the row key (hashed URL), not the content
            return row.key();
        });

        context.output("Loaded " + crawlData.count() + " row keys, building inverted index...\n");

        // Use mapPartitions to process pages and write index entries directly to KVS
        // Workers fetch content directly from KVS to avoid memory issues
        FlameRDD result = crawlData.mapPartitions(rowKeyIterator -> {
            KVSClient workerKvs = new KVSClient(kvsCoordinator);

            // Load English dictionary once per worker (~40 MB in memory)
            Set<String> dictionary = loadDictionary(DICTIONARY_PATH);

            // Build local inverted index for this partition
            // Map: word -> Map<url, count>
            Map<String, Map<String, Integer>> localIndex = new HashMap<>();
            // Batch for crawl table updates (text and title)
            List<Row> crawlBatch = new ArrayList<>();
            int pagesProcessed = 0;

            while (rowKeyIterator.hasNext()) {
                String hashedUrl = rowKeyIterator.next();

                // Fetch row directly from KVS - this keeps memory bounded
                Row crawlRow = workerKvs.getRow(CRAWL_TABLE, hashedUrl);
                if (crawlRow == null) {
                    continue;
                }

                String url = crawlRow.get("url");
                String htmlContent = crawlRow.get("content");
                if (url == null || htmlContent == null) {
                    continue;
                }

                // Unescape HTML entities (loop to handle double encoding)
                String unescaped = htmlContent;
                String prev;
                do {
                    prev = unescaped;
                    unescaped = StringEscapeUtils.unescapeHtml4(unescaped);
                } while (!unescaped.equals(prev));

                // Extract title before stripping HTML
                String title = extractTitle(unescaped, url);

                // Strip HTML to get plain text
                String plainText = stripHtmlTags(unescaped);

                // Store extracted text and title back to pt-crawl
                Row updateRow = new Row(hashedUrl);
                updateRow.put("text", plainText);
                updateRow.put("title", title);
                crawlBatch.add(updateRow);

                // Write crawl batch periodically
                if (crawlBatch.size() >= BATCH_SIZE) {
                    workerKvs.putRowBatch(CRAWL_TABLE, crawlBatch, true);
                    crawlBatch.clear();
                }

                // Tokenize and count occurrences (filtered by English dictionary)
                Map<String, Integer> wordCounts = tokenize(plainText, dictionary);

                // Add to local index
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    String word = entry.getKey();
                    int count = entry.getValue();

                    localIndex.computeIfAbsent(word, k -> new HashMap<>())
                            .put(url, count);
                }

                pagesProcessed++;
            }

            // Write remaining crawl batch
            if (!crawlBatch.isEmpty()) {
                workerKvs.putRowBatch(CRAWL_TABLE, crawlBatch, true);
            }

            // Write local index to KVS in batches
            List<Row> indexBatch = new ArrayList<>();
            int wordsWritten = 0;

            for (Map.Entry<String, Map<String, Integer>> wordEntry : localIndex.entrySet()) {
                String word = wordEntry.getKey();
                Map<String, Integer> urlCounts = wordEntry.getValue();

                Row row = new Row(word);
                for (Map.Entry<String, Integer> urlEntry : urlCounts.entrySet()) {
                    String entryUrl = urlEntry.getKey();
                    int count = urlEntry.getValue();
                    row.put(entryUrl, String.valueOf(count));
                }

                indexBatch.add(row);
                wordsWritten++;

                if (indexBatch.size() >= BATCH_SIZE) {
                    workerKvs.putRowBatch(INDEX_TABLE, indexBatch, true);
                    indexBatch.clear();
                }
            }

            // Write remaining index batch
            if (!indexBatch.isEmpty()) {
                workerKvs.putRowBatch(INDEX_TABLE, indexBatch, true);
            }

            // Return stats as single element
            List<String> stats = new ArrayList<>();
            stats.add("processed:" + pagesProcessed + ",words:" + wordsWritten);
            return stats.iterator();
        });

        // Collect stats
        List<String> stats = result.collect();
        int totalPages = 0;
        int totalWords = 0;
        for (String stat : stats) {
            String[] parts = stat.split(",");
            for (String part : parts) {
                if (part.startsWith("processed:")) {
                    totalPages += Integer.parseInt(part.substring("processed:".length()));
                } else if (part.startsWith("words:")) {
                    totalWords += Integer.parseInt(part.substring("words:".length()));
                }
            }
        }

        // Clean up
        crawlData.destroy();
        result.destroy();

        long endTime = System.currentTimeMillis();
        int indexSize = kvs.count(INDEX_TABLE);

        context.output("\n=== Indexer Complete ===\n");
        context.output("Pages processed: " + totalPages + "\n");
        context.output("Word entries written: " + totalWords + "\n");
        context.output("Unique words in index: " + indexSize + "\n");
        context.output("Time taken: " + (endTime - startTime) / 1000 + " seconds\n");
    }

    /**
     * Strip HTML tags from content, preserving text.
     */
    static String stripHtmlTags(String html) {
        // Remove non-content sections entirely
        String result = html;
        result = SCRIPT_PATTERN.matcher(result).replaceAll(" ");
        result = STYLE_PATTERN.matcher(result).replaceAll(" ");
        result = NOSCRIPT_PATTERN.matcher(result).replaceAll(" ");
        result = SVG_PATTERN.matcher(result).replaceAll(" ");
        result = HEAD_PATTERN.matcher(result).replaceAll(" ");
        result = COMMENT_PATTERN.matcher(result).replaceAll(" ");

        // Replace tags with spaces
        result = HTML_TAG_PATTERN.matcher(result).replaceAll(" ");

        // Normalize whitespace
        return WHITESPACE_PATTERN.matcher(result).replaceAll(" ").trim();
    }

    /**
     * Extract title from HTML content.
     * Returns the URL as fallback if no title found.
     */
    static String extractTitle(String html, String fallbackUrl) {
        Matcher matcher = TITLE_PATTERN.matcher(html);
        if (matcher.find()) {
            String title = matcher.group(1).trim();
            if (!title.isEmpty()) {
                return title;
            }
        }
        return fallbackUrl;
    }

    /**
     * Tokenize text into words, stem them, and count occurrences.
     * Only includes words found in the English dictionary.
     * Returns a map of stemmed word -> occurrence count.
     */
    static Map<String, Integer> tokenize(String text, Set<String> dictionary) {
        Map<String, Integer> wordCounts = new HashMap<>();
        Matcher matcher = WORD_PATTERN.matcher(text.toLowerCase());
        englishStemmer stemmer = new englishStemmer();

        while (matcher.find()) {
            String word = matcher.group();
            if (word.length() < MIN_WORD_LENGTH || word.length() > MAX_WORD_LENGTH) {
                continue;
            }

            // Skip words not in the English dictionary
            if (!dictionary.contains(word)) {
                continue;
            }

            // Stem the word using Snowball stemmer
            stemmer.setCurrent(word);
            stemmer.stem();
            String stemmedWord = stemmer.getCurrent();

            wordCounts.merge(stemmedWord, 1, Integer::sum);
        }

        return wordCounts;
    }

    /**
     * Load English dictionary from file into a HashSet.
     * Returns an empty set if the file cannot be loaded.
     */
    static Set<String> loadDictionary(String path) {
        Set<String> dictionary = new HashSet<>();
        try {
            List<String> lines = Files.readAllLines(Paths.get(path));
            for (String line : lines) {
                String word = line.trim().toLowerCase();
                if (!word.isEmpty()) {
                    dictionary.add(word);
                }
            }
            logger.info("Loaded " + dictionary.size() + " words from dictionary");
        } catch (IOException e) {
            logger.error("Failed to load dictionary from " + path + ": " + e.getMessage());
        }
        return dictionary;
    }
}
