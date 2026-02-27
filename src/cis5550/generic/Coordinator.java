package cis5550.generic;

import java.util.*;
import java.util.concurrent.*;

import cis5550.tools.*;
import static cis5550.webserver.Server.*;

public class Coordinator {
    final static Logger logger = Logger.getLogger(Coordinator.class);
    static Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    static class WorkerInfo {
        String id;
        String ip;
        int port;
        long lastPing;

        WorkerInfo(String id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.lastPing = System.currentTimeMillis();
        }

        void updatePing() {
            this.lastPing = System.currentTimeMillis();
        }

        boolean isExpired() {
            return (System.currentTimeMillis() - this.lastPing) > 15000;
        }
    }

    public static List<String> getWorkers() {
        removeExpiredWorkers();
        List<String> result = new ArrayList<>();
        for (WorkerInfo worker : workers.values()) {
            result.add(worker.ip + ":" + worker.port);
        }
        return result;
    }

    protected static String workerTable() {
        removeExpiredWorkers();

        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table border=1><tr><th>ID</th><th>IP:Port</th><th>Last Ping></th></tr>");
        for (WorkerInfo worker : workers.values()) {
            String ipPort = worker.ip + ":" + worker.port;
            String link = "<a href='http://" + ipPort + "'>" + ipPort + "</a>";
            htmlTable.append("<tr><td>").append(worker.id).append("</td>")
                    .append("<td>").append(link).append("</td>")
                    .append("<td>").append(new Date(worker.lastPing)).append("</td></tr>");
        }
        htmlTable.append("</table>");

        return htmlTable.toString();
    }

    protected static void registerRoutes() {
        get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String portArg = req.queryParams("port");
            String ip = req.ip();

            if (id == null || portArg == null) {
                logger.warn("Received ping with missing parameters: id=" + id + ", port=" + portArg);
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            int port;
            try {
                port = Integer.parseInt(portArg);
            } catch (NumberFormatException e) {
                logger.warn("Received ping with invalid port number: " + portArg);
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            WorkerInfo existingWorker = workers.get(id);
            if (existingWorker == null) {
                workers.put(id, new WorkerInfo(id, ip, port));
                logger.info("Registered new worker: " + id + "(" + ip + ":" + port + ")");
            } else {
                existingWorker.ip = ip;
                existingWorker.port = port;
                existingWorker.updatePing();
            }

            return "OK";
        });

        get("/workers", (req, res) -> {
            removeExpiredWorkers();
            StringBuilder response = new StringBuilder();
            response.append(workers.size()).append("\n");
            for (WorkerInfo worker : workers.values()) {
                response.append(worker.id + "," + worker.ip + ":" + worker.port + "\n");
            }
            return response.toString();
        });
    }

    static void removeExpiredWorkers() {
        workers.entrySet().removeIf(entry -> {
            WorkerInfo worker = entry.getValue();
            if (worker.isExpired()) {
                logger.info("Removing expired worker: " + worker.id + "(" + worker.ip + ":" + worker.port + ")");
                return true;
            }
            return false;
        });
    }
}