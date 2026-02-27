package cis5550.kvs;

import cis5550.tools.*;
import static cis5550.webserver.Server.*;

class Coordinator extends cis5550.generic.Coordinator {
    static final Logger logger = Logger.getLogger(Coordinator.class);

    static int port;

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("Usage: java cis5550.kvs.Coordinator <port>");
            System.exit(1);
            return;
        }

        port = Integer.parseInt(args[0]);

        port(port);
        registerRoutes();

        get("/", (req, res) -> {
            res.type("text/html");

            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>").append("KVS coordinator").append("</title></head>")
                    .append("<body><h1>").append("KVS coordinator").append("</h1>");
            html.append(workerTable());
            html.append("</body></html>");

            return html.toString();
        });
    }
}