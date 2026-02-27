package cis5550.flame;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import cis5550.flame.FlameContext.*;
import cis5550.flame.FlamePairRDD.*;
import cis5550.flame.FlameRDD.*;
import cis5550.kvs.*;
import cis5550.tools.*;
import cis5550.webserver.*;
import static cis5550.webserver.Server.*;
import static cis5550.tools.KeyGenerator.*;

class Worker extends cis5550.generic.Worker {
    static final Logger logger = Logger.getLogger(Worker.class);
    static int port;
    static String coordinator;

    static class FlameOperationContext {
        final String operation;
        final String inputTable;
        final String outputTable;
        final KVSClient kvs;
        final String fromKey;
        final String toKeyExclusive;
        final File jarFile;

        FlameOperationContext(Request req, File jarFile) {
            this.operation = req.requestMethod() + " " + req.url();
            this.inputTable = req.queryParams("inputTable");
            this.outputTable = req.queryParams("outputTable");
            this.fromKey = req.queryParams("fromKey");
            this.toKeyExclusive = req.queryParams("toKeyExclusive");
            this.kvs = new KVSClient(req.queryParams("kvsCoordinator"));
            this.jarFile = jarFile;
        }

        Iterator<Row> scanInputTable() throws IOException {
            return kvs.scan(inputTable, fromKey, toKeyExclusive);
        }

        @SuppressWarnings("unchecked")
        <T> T deserializeLambda(byte[] bytes) throws Exception {
            try {
                return (T) Serializer.byteArrayToObject(bytes, jarFile);
            } catch (Exception e) {
                logger.error(String.format("Failed to deserialize lambda: operation=%s, jar=%s", operation, jarFile), e);
                throw e;
            }
        }
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            logger.error("Usage: java cis5550.flame.Worker <port> <coordinator_ip:port>");
            System.exit(1);
        }

        port = Integer.parseInt(args[0]);
        coordinator = args[1];
        port(port);
        startPingThread(port, generateRandomWorkerId(), coordinator);

        final Path myJAR = Paths.get("__worker" + port + "-current.jar");

        post("/useJAR", (req, res) -> {
            long startTime = System.currentTimeMillis();
            logger.debug("POST /useJAR: started");

            byte[] jarBytes = req.bodyAsBytes();
            Files.write(myJAR, jarBytes);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.info(String.format("POST /useJAR: received new JAR (%d bytes) in %dms", jarBytes.length, duration));

            return "OK";
        });

        post("/fromTable", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /fromTable: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            RowToString lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            int outputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                inputCount++;
                String value = lambda.op(row);
                if (value != null) {
                    Row outputRow = new Row(row.key());
                    outputRow.put("value", value);
                    batch.add(outputRow);

                    // Write batches periodically to avoid OOM
                    if (batch.size() >= 1000) {
                        context.kvs.putRowBatch(context.outputTable, batch, false);
                        outputCount += batch.size();
                        batch.clear();
                    }
                }
            }
            // Write remaining batch
            if (!batch.isEmpty()) {
                context.kvs.putRowBatch(context.outputTable, batch, false);
                outputCount += batch.size();
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /fromTable: %d inputs -> %d outputs in %dms", inputCount, outputCount, duration));

            return "OK";
        });

        post("/rdd/flatMap", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/flatMap: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            StringToIterable lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    Iterable<String> results = lambda.op(value);
                    if (results != null) {
                        for (String result : results) {
                            Row outputRow = new Row(generateUniqueKey());
                            outputRow.put("value", result);
                            batch.add(outputRow);
                        }
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/flatMap: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/rdd/flatMapToPair", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/flatMapToPair: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            StringToPairIterable lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    Iterable<FlamePair> results = lambda.op(value);
                    if (results != null) {
                        for (FlamePair pair : results) {
                            Row outputRow = new Row(pair._1());
                            outputRow.put(generateUniqueKey(), pair._2());
                            batch.add(outputRow);
                        }
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/flatMapToPair: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/rdd/mapToPair", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/mapToPair: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            StringToPair lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    FlamePair pair = lambda.op(value);
                    if (pair != null) {
                        Row outputRow = new Row(pair._1());
                        outputRow.put(generateUniqueKey(), pair._2());
                        batch.add(outputRow);
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/mapToPair: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/rdd/fold", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/fold: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            TwoStringsToString lambda = context.deserializeLambda(req.bodyAsBytes());

            Iterator<Row> rows = context.scanInputTable();
            String accumulator = req.queryParams("zeroElement");

            int inputCount = 0;
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    accumulator = lambda.op(accumulator, value);
                }
            }

            context.kvs.put(context.outputTable, String.valueOf(port), "value", accumulator);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/fold: %d inputs in %dms", inputCount, duration));

            return "OK";
        });

        post("/rdd/distinct", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/distinct: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    Row outputRow = new Row(Hasher.hash(value));
                    outputRow.put("value", value);
                    batch.add(outputRow);
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/distinct: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/rdd/subtract", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());
            String otherInputTable = req.queryParams("otherInputTable");

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/subtract: started, input=%s, otherInput=%s, output=%s, range=[%s, %s)",
                    context.inputTable, otherInputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            Set<String> otherValues = new HashSet<>();
            Iterator<Row> otherRows = context.kvs.scan(otherInputTable);
            while (otherRows.hasNext()) {
                Row row = otherRows.next();
                String value = row.get("value");
                if (value != null) {
                    otherValues.add(value);
                }
            }

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                if (value != null) {
                    inputCount++;
                    if (!otherValues.contains(value)) {
                        Row outputRow = new Row(generateUniqueKey());
                        outputRow.put("value", value);
                        batch.add(outputRow);
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/subtract: %d inputs - %d others -> %d outputs in %dms",
                    inputCount, otherValues.size(), batch.size(), duration));

            return "OK";
        });

        post("/rdd/mapPartitions", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /rdd/mapPartitions: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            IteratorToIterator lambda = context.deserializeLambda(req.bodyAsBytes());

            // Create an iterator over all values in this partition
            Iterator<Row> rows = context.scanInputTable();
            final int[] inputCount = {0};
            Iterator<String> valueIterator = new Iterator<String>() {
                private String nextValue = null;
                private boolean hasNextCalled = false;

                @Override
                public boolean hasNext() {
                    if (!hasNextCalled) {
                        nextValue = findNextValue();
                        hasNextCalled = true;
                    }
                    return nextValue != null;
                }

                @Override
                public String next() {
                    if (!hasNextCalled) {
                        nextValue = findNextValue();
                    }
                    hasNextCalled = false;
                    String result = nextValue;
                    nextValue = null;
                    return result;
                }

                private String findNextValue() {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String value = row.get("value");
                        if (value != null) {
                            inputCount[0]++;
                            return value;
                        }
                    }
                    return null;
                }
            };

            // Invoke the lambda with the partition's iterator
            Iterator<String> resultIterator = lambda.op(valueIterator);

            // Collect results into output table
            List<Row> batch = new ArrayList<>();
            while (resultIterator != null && resultIterator.hasNext()) {
                String result = resultIterator.next();
                if (result != null) {
                    Row outputRow = new Row(generateUniqueKey());
                    outputRow.put("value", result);
                    batch.add(outputRow);
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /rdd/mapPartitions: %d inputs -> %d outputs in %dms", inputCount[0], batch.size(), duration));

            return "OK";
        });

        post("/pairrdd/flatMap", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /pairrdd/flatMap: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            PairToStringIterable lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                Set<String> columns = row.columns();

                for (String column : columns) {
                    String value = row.get(column);
                    if (value != null) {
                        inputCount++;
                        FlamePair pair = new FlamePair(row.key(), value);
                        Iterable<String> results = lambda.op(pair);
                        if (results != null) {
                            for (String result : results) {
                                Row outputRow = new Row(generateUniqueKey());
                                outputRow.put("value", result);
                                batch.add(outputRow);
                            }
                        }
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /pairrdd/flatMap: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/pairrdd/flatMapToPair", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /pairrdd/flatMapToPair: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            PairToPairIterable lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                Set<String> columns = row.columns();

                for (String column : columns) {
                    String value = row.get(column);
                    if (value != null) {
                        inputCount++;
                        FlamePair inputPair = new FlamePair(row.key(), value);
                        Iterable<FlamePair> results = lambda.op(inputPair);
                        if (results != null) {
                            for (FlamePair outputPair : results) {
                                Row outputRow = new Row(outputPair._1());
                                outputRow.put(generateUniqueKey(), outputPair._2());
                                batch.add(outputRow);
                            }
                        }
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /pairrdd/flatMapToPair: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/pairrdd/foldByKey", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /pairrdd/foldByKey: started, input=%s, output=%s, range=[%s, %s)",
                    context.inputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            TwoStringsToString lambda = context.deserializeLambda(req.bodyAsBytes());

            int inputCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                inputCount++;
                String accumulator = req.queryParams("zeroElement");

                Set<String> columns = row.columns();
                for (String column : columns) {
                    String value = row.get(column);
                    if (value != null) {
                        accumulator = lambda.op(accumulator, value);
                    }
                }

                Row outputRow = new Row(row.key());
                outputRow.put("value", accumulator);
                batch.add(outputRow);
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /pairrdd/foldByKey: %d inputs -> %d outputs in %dms", inputCount, batch.size(), duration));

            return "OK";
        });

        post("/pairrdd/join", (req, res) -> {
            FlameOperationContext context = new FlameOperationContext(req, myJAR.toFile());
            String otherInputTable = req.queryParams("otherInputTable");

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("POST /pairrdd/join: started, input=%s, otherInput=%s, output=%s, range=[%s, %s)",
                    context.inputTable, otherInputTable, context.outputTable, context.fromKey, context.toKeyExclusive));

            int inputCount = 0;
            int joinCount = 0;
            List<Row> batch = new ArrayList<>();
            Iterator<Row> rows = context.scanInputTable();
            while (rows.hasNext()) {
                Row row = rows.next();
                inputCount++;
                Row otherRow = context.kvs.getRow(otherInputTable, row.key());
                if (otherRow != null) {
                    joinCount++;
                    Set<String> columns = row.columns();
                    Set<String> otherColumns = otherRow.columns();

                    for (String column : columns) {
                        String value = row.get(column);
                        if (value != null) {
                            for (String otherColumn : otherColumns) {
                                String otherValue = otherRow.get(otherColumn);
                                if (otherValue != null) {
                                    String joinedValue = value + "," + otherValue;
                                    Row outputRow = new Row(row.key());
                                    outputRow.put(generateUniqueKey(), joinedValue);
                                    batch.add(outputRow);
                                }
                            }
                        }
                    }
                }
            }
            context.kvs.putRowBatch(context.outputTable, batch, false);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("POST /pairrdd/join: %d inputs, %d joins -> %d outputs in %dms",
                    inputCount, joinCount, batch.size(), duration));

            return "OK";
        });
    }
}