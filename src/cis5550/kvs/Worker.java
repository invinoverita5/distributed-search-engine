package cis5550.kvs;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.*;

import cis5550.tools.*;
import static cis5550.webserver.Server.*;

public class Worker extends cis5550.generic.Worker {
    static final Logger logger = Logger.getLogger(Worker.class);

    static final Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();
    static final Map<String, Object> rowLocks = new ConcurrentHashMap<>();

    static int port;
    static String storageDir;
    static String coordinator;

    public static void main(String[] args) {
        if (args.length != 3) {
            logger.error("Usage: java cis5550.kvs.Worker <port> <storage_dir> <coordinator_ip:port>");
            System.exit(1);
            return;
        }

        port = Integer.parseInt(args[0]);
        storageDir = args[1];
        coordinator = args[2];

        port(port);

        String workerId = getKVSWorkerId(storageDir);
        startPingThread(port, workerId, coordinator);

        get("/", (req, res) -> {
            List<String> tableNames = getTableNames();
            Collections.sort(tableNames);

            res.type("text/html");
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>KVS Worker</title></head><body>");
            html.append("<h1>KVS Worker</h1>");
            html.append("<table border='1'>");
            html.append("<tr><th>Table Name</th><th>Number of Rows</th></tr>");

            for (String tableName : tableNames) {
                int numRows = getRowCount(tableName);
                html.append("<tr>");
                html.append("<td><a href='/view/").append(tableName).append("'>").append(tableName)
                        .append("</a></td>");
                html.append("<td>").append(numRows).append("</td>");
                html.append("</tr>");
            }

            html.append("</table>");
            html.append("</body></html>");
            return html.toString();
        });

        get("/view/:table", (req, res) -> {
            String tableName = req.params("table");
            String startRow = req.queryParams("startRow");
            int pageSize = 10;

            // Use paginated fetch for efficiency
            List<Row> rows = getRowsPaginated(tableName, startRow, pageSize + 1);

            boolean hasNextPage = rows.size() > pageSize;
            String nextStartRow = hasNextPage ? rows.get(pageSize).key() : null;
            if (hasNextPage) {
                rows = rows.subList(0, pageSize);
            }

            List<String> columns = getColumns(rows, true);

            res.type("text/html");
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>Table: ").append(tableName).append("</title>");
            html.append("<style>");
            html.append("td { max-height: 150px; max-width: 400px; overflow: auto; vertical-align: top; }");
            html.append(
                    "td > div { max-height: 150px; overflow: auto; white-space: pre-wrap; word-break: break-word; }");
            html.append("</style>");
            html.append("</head><body>");
            html.append("<h1>Table: ").append(tableName).append("</h1>");

            // Navigation at the top
            html.append("<div style='margin-bottom: 10px;'>");
            if (hasNextPage) {
                html.append("<a href='/view/").append(tableName)
                        .append("?startRow=").append(java.net.URLEncoder.encode(nextStartRow, "UTF-8"))
                        .append("'><button>Next</button></a>");
            }
            html.append("</div>");

            html.append("<table border='1'>");
            html.append("<tr><th>Row Key</th>");
            for (String column : columns) {
                html.append("<th>").append(column).append("</th>");
            }
            html.append("</tr>");
            for (Row row : rows) {
                html.append("<tr>");
                html.append("<td><div>").append(row.key()).append("</div></td>");
                for (String column : columns) {
                    String value = row.get(column);
                    html.append("<td><div>").append(value != null ? value : "").append("</div></td>");
                }
                html.append("</tr>");
            }
            html.append("</table>");
            html.append("</body></html>");

            return html.toString();
        });

        get("/tables", (req, res) -> {
            res.type("text/plain");
            StringBuilder result = new StringBuilder();
            for (String tableName : getTableNames()) {
                result.append(tableName).append("\n");
            }
            return result.toString();
        });

        put("/data/:table", (req, res) -> {
            String tableName = req.params("table");

            try (InputStream in = new ByteArrayInputStream(req.bodyAsBytes())) {
                Row row = Row.readFrom(in);
                if (row == null) {
                    res.status(400, "Bad Request");
                    return "Bad Request";
                }
                putRow(tableName, row);
            } catch (Exception e) {
                logger.error("Error parsing row data", e);
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            res.type("text/plain");
            return "OK";
        });

        put("/batch/:table", (req, res) -> {
            String tableName = req.params("table");
            boolean merge = "true".equals(req.queryParams("merge"));

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("PUT /batch/%s: started (merge=%s)", tableName, merge));

            int rowCount = 0;
            try (InputStream in = new ByteArrayInputStream(req.bodyAsBytes())) {
                while (true) {
                    try {
                        Row row = Row.readFrom(in);
                        if (row == null)
                            break;
                        if (merge) {
                            mergeRow(tableName, row);
                        } else {
                            putRow(tableName, row);
                        }
                        rowCount++;
                    } catch (Exception e) {
                        logger.error("Error processing row in batch", e);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing batch", e);
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("PUT /batch/%s: %d rows in %dms", tableName, rowCount, duration));

            res.type("text/plain");
            return "OK";
        });

        get("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("GET /data/%s: started", tableName));

            if (!tableExists(tableName)) {
                logger.warn(String.format("Scan failed, table not found: %s", tableName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.type("text/plain");
            int rowCount = 0;

            // Stream rows one at a time to avoid OOM on large tables
            if (isPersistent(tableName)) {
                Path tablePath = Paths.get(storageDir, tableName);
                // Get sorted list of file names (lightweight - just filenames, not content)
                List<String> fileNames = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(tablePath,
                        path -> !path.getFileName().toString().startsWith("."))) {
                    for (Path path : stream) {
                        fileNames.add(path.getFileName().toString());
                    }
                }
                Collections.sort(fileNames);

                // Stream rows one at a time within the key range
                for (String fileName : fileNames) {
                    String rowKey = KeyEncoder.decode(fileName);
                    if (startRow != null && rowKey.compareTo(startRow) < 0) {
                        continue;
                    }
                    if (endRowExclusive != null && rowKey.compareTo(endRowExclusive) >= 0) {
                        break; // Since sorted, no more rows will match
                    }
                    Path rowPath = tablePath.resolve(fileName);
                    try (InputStream in = Files.newInputStream(rowPath)) {
                        Row row = Row.readFrom(in);
                        if (row != null) {
                            res.write(row.toByteArray());
                            rowCount++;
                        }
                    } catch (Exception e) {
                        logger.error(String.format("Error reading row file: %s", rowPath), e);
                    }
                }
            } else {
                // In-memory table - still need to sort but can stream
                Map<String, Row> table = tables.get(tableName);
                if (table != null) {
                    List<String> keys = new ArrayList<>(table.keySet());
                    Collections.sort(keys);
                    for (String key : keys) {
                        if (startRow != null && key.compareTo(startRow) < 0) {
                            continue;
                        }
                        if (endRowExclusive != null && key.compareTo(endRowExclusive) >= 0) {
                            break;
                        }
                        Row row = table.get(key);
                        if (row != null) {
                            res.write(row.toByteArray());
                            rowCount++;
                        }
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("GET /data/%s: %d rows in %dms", tableName, rowCount, duration));

            return null;
        });

        get("/data/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");

            if (!tableExists(tableName)) {
                logger.warn(String.format("Get row failed, table not found: %s", tableName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            Row row = getRow(tableName, rowKey);
            if (row == null) {
                logger.warn(String.format("Get row failed, row not found: %s/%s", tableName, rowKey));
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.bodyAsBytes(row.toByteArray());
            return null;
        });

        put("/data/:table/:row/:column", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");
            String columnName = req.params("column");

            putCell(tableName, rowKey, columnName, req.bodyAsBytes());

            res.type("text/plain");
            return "OK";
        });

        get("/data/:table/:row/:column", (req, res) -> {
            String tableName = req.params("table");
            String rowKey = req.params("row");
            String columnName = req.params("column");

            Row row = getRow(tableName, rowKey);
            if (row == null) {
                logger.warn(String.format("Get column failed, row not found: %s/%s", tableName, rowKey));
                res.status(404, "Not Found");
                return "Not Found";
            }

            byte[] data = row.getBytes(columnName);
            if (data == null) {
                logger.warn(String.format("Get column failed, column not found: %s/%s/%s", tableName, rowKey, columnName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.type("application/octet-stream");
            res.bodyAsBytes(data);
            return null;
        });

        put("/rename/:table", (req, res) -> {
            String oldTableName = req.params("table");
            String newTableName = req.body();

            logger.debug(String.format("PUT /rename/%s -> %s: started", oldTableName, newTableName));

            if (!tableExists(oldTableName)) {
                logger.warn(String.format("Rename failed, table not found: %s", oldTableName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            if (tableExists(newTableName)) {
                logger.warn(String.format("Rename failed, table already exists: %s", newTableName));
                res.status(409, "Conflict");
                return "Conflict";
            }

            if (isPersistent(oldTableName) != isPersistent(newTableName)) {
                logger.warn(String.format("Rename failed for %s -> %s, can't convert in-memory tables to persistent tables and vice versa",
                        oldTableName, newTableName));
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            logger.info(String.format("Rename table succeeded: %s -> %s", oldTableName, newTableName));
            renameTable(oldTableName, newTableName);

            res.type("text/plain");
            return "OK";
        });

        put("/delete/:table", (req, res) -> {
            String tableName = req.params("table");

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("PUT /delete/%s: started", tableName));

            if (!tableExists(tableName)) {
                logger.warn(String.format("Delete failed, table not found: %s", tableName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            int rowCount = deleteTable(tableName);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("PUT /delete/%s: %d rows deleted in %dms", tableName, rowCount, duration));

            res.type("text/plain");
            return "OK";
        });

        get("/count/:table", (req, res) -> {
            String tableName = req.params("table");

            long startTime = System.currentTimeMillis();
            logger.debug(String.format("GET /count/%s: started", tableName));

            if (!tableExists(tableName)) {
                logger.warn(String.format("Count failed, table not found: %s", tableName));
                res.status(404, "Not Found");
                return "Not Found";
            }

            int rowCount = getRowCount(tableName);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            logger.debug(String.format("GET /count/%s: %d rows in %dms", tableName, rowCount, duration));

            res.type("text/plain");
            return String.valueOf(rowCount);
        });
    }

    static List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>();
        tableNames.addAll(tables.keySet());
        try (DirectoryStream<Path> tablePaths = Files.newDirectoryStream(Paths.get(storageDir))) {
            for (Path tablePath : tablePaths) {
                if (Files.isDirectory(tablePath)) {
                    tableNames.add(tablePath.getFileName().toString());
                }
            }
        } catch (IOException e) {
            logger.error(String.format("Failed to read tables from disk: %s", storageDir), e);
        }
        return tableNames;
    }

    static void putCell(String tableName, String rowKey, String columnName, byte[] data) {
        if (isPersistent(tableName)) {
            String lockKey = tableName + "/" + rowKey;
            Object lock = rowLocks.computeIfAbsent(lockKey, k -> new Object());
            synchronized (lock) {
                try {
                    Path tablePath = Paths.get(storageDir, tableName);
                    if (!Files.exists(tablePath)) {
                        logger.info(String.format("Creating new persistent table: %s", tableName));
                        Files.createDirectories(tablePath);
                    }
                    Path rowPath = tablePath.resolve(KeyEncoder.encode(rowKey));
                    Row row;
                    if (Files.exists(rowPath)) {
                        try (InputStream in = Files.newInputStream(rowPath)) {
                            row = Row.readFrom(in);
                        }
                    } else {
                        row = new Row(rowKey);
                    }
                    row.put(columnName, data);
                    Files.write(rowPath, row.toByteArray());
                } catch (Exception e) {
                    logger.error("Error writing row to disk", e);
                }
            }
        } else {
            Map<String, Row> table = tables.computeIfAbsent(tableName, k -> {
                logger.info(String.format("Creating new in-memory table: %s", tableName));
                return new ConcurrentHashMap<>();
            });
            Row row = table.computeIfAbsent(rowKey, k -> {
                return new Row(k);
            });
            row.put(columnName, data);
        }
    }

    static void putRow(String tableName, Row row) {
        if (isPersistent(tableName)) {
            try {
                Path tablePath = Paths.get(storageDir, tableName);
                if (!Files.exists(tablePath)) {
                    logger.info(String.format("Creating new persistent table: %s", tableName));
                    Files.createDirectories(tablePath);
                }
                Path rowPath = tablePath.resolve(KeyEncoder.encode(row.key()));
                Files.write(rowPath, row.toByteArray());
            } catch (Exception e) {
                logger.error("Error writing row to disk", e);
            }
        } else {
            Map<String, Row> table = tables.computeIfAbsent(tableName, k -> {
                logger.info(String.format("Creating new in-memory table: %s", tableName));
                return new ConcurrentHashMap<>();
            });
            table.put(row.key(), row);
        }
    }

    static void mergeRow(String tableName, Row newRow) {
        if (isPersistent(tableName)) {
            String lockKey = tableName + "/" + newRow.key();
            Object lock = rowLocks.computeIfAbsent(lockKey, k -> new Object());
            synchronized (lock) {
                try {
                    Path tablePath = Paths.get(storageDir, tableName);
                    if (!Files.exists(tablePath)) {
                        logger.info(String.format("Creating new persistent table: %s", tableName));
                        Files.createDirectories(tablePath);
                    }
                    Path rowPath = tablePath.resolve(KeyEncoder.encode(newRow.key()));
                    Row merged = newRow;
                    if (Files.exists(rowPath)) {
                        try (InputStream in = Files.newInputStream(rowPath)) {
                            Row existing = Row.readFrom(in);
                            if (existing != null) {
                                for (String col : newRow.columns()) {
                                    existing.put(col, newRow.getBytes(col));
                                }
                                merged = existing;
                            }
                        }
                    }
                    Files.write(rowPath, merged.toByteArray());
                } catch (Exception e) {
                    logger.error("Error merging row to disk", e);
                }
            }
        } else {
            Map<String, Row> table = tables.computeIfAbsent(tableName, k -> {
                logger.info(String.format("Creating new in-memory table: %s", tableName));
                return new ConcurrentHashMap<>();
            });
            table.compute(newRow.key(), (k, existing) -> {
                if (existing == null) {
                    return newRow;
                }
                for (String col : newRow.columns()) {
                    existing.put(col, newRow.getBytes(col));
                }
                return existing;
            });
        }
    }

    static Row getRow(String tableName, String rowKey) {
        if (isPersistent(tableName)) {
            String rowFileName = KeyEncoder.encode(rowKey);
            Path rowPath = Paths.get(storageDir, tableName, rowFileName);
            try (InputStream in = Files.newInputStream(rowPath)) {
                return Row.readFrom(in);
            } catch (NoSuchFileException e) {
                // Row doesn't exist
                return null;
            } catch (IOException e) {
                logger.error(String.format("Failed to read row: %s (table: %s, key: %s)", rowPath, tableName, rowKey), e);
                return null;
            } catch (Exception e) {
                logger.error(String.format("Error parsing row file: %s (table: %s, key: %s)", rowPath, tableName, rowKey), e);
                return null;
            }
        } else {
            Map<String, Row> table = tables.get(tableName);
            if (table == null) {
                return null;
            }
            return table.get(rowKey);
        }
    }

    static List<Row> getRowsPaginated(String tableName, String startRow, int limit) {
        List<Row> rows = new ArrayList<>();

        if (isPersistent(tableName)) {
            Path tablePath = Paths.get(storageDir, tableName);
            try {
                // Get sorted list of file names (which correspond to encoded row keys)
                List<String> fileNames = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(tablePath,
                        path -> !path.getFileName().toString().startsWith("."))) {
                    for (Path path : stream) {
                        fileNames.add(path.getFileName().toString());
                    }
                }
                Collections.sort(fileNames);

                // Find starting point and collect rows
                for (String fileName : fileNames) {
                    String rowKey = KeyEncoder.decode(fileName);
                    if (startRow != null && rowKey.compareTo(startRow) < 0) {
                        continue;
                    }
                    if (rows.size() >= limit) {
                        break;
                    }
                    Path rowPath = tablePath.resolve(fileName);
                    try (InputStream in = Files.newInputStream(rowPath)) {
                        Row row = Row.readFrom(in);
                        if (row != null) {
                            rows.add(row);
                        }
                    } catch (Exception e) {
                        logger.error(String.format("Error reading row file: %s", rowPath), e);
                    }
                }
            } catch (IOException e) {
                logger.error(String.format("Failed to list rows in table: %s", tableName), e);
            }
        } else {
            // In-memory table
            Map<String, Row> table = tables.get(tableName);
            if (table != null) {
                List<String> keys = new ArrayList<>(table.keySet());
                Collections.sort(keys);
                for (String key : keys) {
                    if (startRow != null && key.compareTo(startRow) < 0) {
                        continue;
                    }
                    if (rows.size() >= limit) {
                        break;
                    }
                    rows.add(table.get(key));
                }
            }
        }

        return rows;
    }

    static List<Row> getRows(String tableName, boolean sortedByKey) {
        List<Row> rows = new ArrayList<>();

        if (isPersistent(tableName)) {
            Path tablePath = Paths.get(storageDir, tableName);
            try (DirectoryStream<Path> rowPaths = Files.newDirectoryStream(tablePath,
                    path -> !path.getFileName().toString().startsWith("."))) {
                for (Path rowPath : rowPaths) {
                    try (InputStream in = Files.newInputStream(rowPath)) {
                        Row row = Row.readFrom(in);
                        rows.add(row);
                    } catch (IOException e) {
                        logger.error(String.format("Failed to read row file: %s", rowPath), e);
                    } catch (Exception e) {
                        logger.error(String.format("Error parsing row from file: %s", rowPath), e);
                    }
                }
            } catch (IOException e) {
                logger.error(String.format("Failed to list rows in table: %s", tableName), e);
            }
        } else {
            Map<String, Row> table = tables.get(tableName);
            if (table != null) {
                rows.addAll(table.values());
            }
        }

        if (sortedByKey) {
            Collections.sort(rows, (r1, r2) -> r1.key().compareTo(r2.key()));
        }
        return rows;
    }

    static int getRowCount(String tableName) {
        if (isPersistent(tableName)) {
            Path tablePath = Paths.get(storageDir, tableName);
            int count = 0;
            try (DirectoryStream<Path> rowFiles = Files.newDirectoryStream(tablePath,
                    path -> !path.getFileName().toString().startsWith("."))) {
                for (Path ignored : rowFiles) {
                    count++;
                }
            } catch (IOException e) {
                logger.error(String.format("Failed to count rows in table: %s", tableName), e);
            }
            return count;
        } else {
            Map<String, Row> table = tables.get(tableName);
            return table != null ? table.size() : 0;
        }
    }

    static List<String> getColumns(List<Row> rows, boolean sortedByKey) {
        Set<String> distinctColumns = new HashSet<>();
        for (Row row : rows) {
            distinctColumns.addAll(row.columns());
        }
        List<String> columns = new ArrayList<>(distinctColumns);
        if (sortedByKey) {
            Collections.sort(columns, (c1, c2) -> c1.compareTo(c2));
        }
        return columns;
    }

    static boolean tableExists(String tableName) {
        if (isPersistent(tableName)) {
            return Files.isDirectory(Paths.get(storageDir, tableName));
        } else {
            return tables.containsKey(tableName);
        }
    }

    static void renameTable(String oldName, String newName) {
        if (isPersistent(oldName) && isPersistent(newName)) {
            Path oldPath = Paths.get(storageDir, oldName);
            Path newPath = Paths.get(storageDir, newName);
            try {
                Files.move(oldPath, newPath);
            } catch (IOException e) {
                logger.error(String.format("Failed to rename table from %s to %s", oldName, newName), e);
            }
        } else if (!isPersistent(oldName) && !isPersistent(newName)) {
            Map<String, Row> table = tables.remove(oldName);
            if (table != null) {
                tables.put(newName, table);
            }
        }
    }

    static int deleteTable(String tableName) {
        if (isPersistent(tableName)) {
            Path tablePath = Paths.get(storageDir, tableName);
            int[] rowCount = {0};
            try {
                Files.walkFileTree(tablePath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (!file.getFileName().toString().startsWith(".")) {
                            rowCount[0]++;
                        }
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                logger.error(String.format("Failed to delete table: %s", tableName), e);
            }
            return rowCount[0];
        } else {
            Map<String, Row> table = tables.remove(tableName);
            return table != null ? table.size() : 0;
        }
    }

    static boolean isPersistent(String tableName) {
        return tableName.startsWith("pt-");
    }

    static String getKVSWorkerId(String storageDir) {
        Path idFilePath = Paths.get(storageDir, "id");

        try {
            return Files.readString(idFilePath).trim();
        } catch (IOException e) {
            // fresh worker; neither the directory nor the ID file exists
        }

        String workerId = generateRandomWorkerId();
        try {
            Files.createDirectories(Paths.get(storageDir));
            Files.writeString(idFilePath, workerId);
        } catch (IOException e) {
            logger.error("Failed to save worker id to file", e);
        }
        return workerId;
    }
}
