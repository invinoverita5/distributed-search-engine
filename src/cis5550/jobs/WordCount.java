package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length < 1) {
            ctx.output("Error: Missing input directory argument\n");
            ctx.output("Usage: WordCount <input-directory>\n");
            return;
        }

        String inputDir = args[0];

        // Discover all .txt files in the input directory
        File dir = new File(inputDir);
        if (!dir.exists() || !dir.isDirectory()) {
            ctx.output("Error: Input directory does not exist: " + inputDir + "\n");
            return;
        }

        List<String> filePaths = new ArrayList<>();
        File[] files = dir.listFiles((d, name) -> name.endsWith(".txt"));

        if (files == null || files.length == 0) {
            ctx.output("Error: No .txt files found in directory: " + inputDir + "\n");
            return;
        }

        for (File file : files) {
            filePaths.add(file.getAbsolutePath());
        }

        ctx.output("Processing " + filePaths.size() + " files...\n");

        // Distribute file paths across workers
        FlameRDD pathsRDD = ctx.parallelize(filePaths);

        // Each worker reads its assigned file(s) and returns lines
        FlameRDD lines = pathsRDD.flatMap(filePath -> {
            List<String> result = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Skip empty lines
                    if (!line.trim().isEmpty()) {
                        result.add(line);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error reading file " + filePath + ": " + e.getMessage());
            }
            return result;
        });

        // Split lines into words
        FlameRDD words = lines.flatMap(line -> {
            // Split on whitespace and remove punctuation
            String[] tokens = line.toLowerCase()
                    .replaceAll("[^a-z0-9\\s]", " ")
                    .split("\\s+");

            List<String> result = new ArrayList<>();
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    result.add(token);
                }
            }
            return result;
        });

        // Map each word to (word, 1)
        FlamePairRDD wordPairs = words.mapToPair(word -> new FlamePair(word, "1"));

        // Reduce by key to count occurrences
        FlamePairRDD counts = wordPairs.foldByKey("0", (a, b) -> {
            int countA = Integer.parseInt(a);
            int countB = Integer.parseInt(b);
            return String.valueOf(countA + countB);
        });

        // Save results to KVS table
        counts.saveAsTable("wordcount-output");

        ctx.output("WordCount completed successfully!\n");
        ctx.output("Results saved to table: wordcount-output\n");
    }
}
