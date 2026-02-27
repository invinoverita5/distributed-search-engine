package cis5550.generic;

import java.io.*;
import java.net.*;
import java.util.*;

import cis5550.tools.*;

public class Worker {
    static final Logger logger = Logger.getLogger(Worker.class);

    protected static void startPingThread(int workerPort, String workerId, String coordinator) {
        String urlString = "http://" + coordinator + "/ping?id=" + workerId + "&port=" + workerPort;

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    URI uri = new URI(urlString);
                    URL url = uri.toURL();
                    url.getContent();

                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Ping thread was interrupted", e);
                } catch (URISyntaxException e) {
                    logger.error(urlString + " could not be parsed as a valid URI reference", e);
                } catch (MalformedURLException e) {
                    logger.error(urlString + " could not be parsed as a valid URL", e);
                } catch (IOException e) {
                    logger.error("An I/O exception occurred while pinging " + urlString, e);
                }
            }
        });

        // ping thread should not keep application running if worker has exited
        t.setDaemon(true);

        t.start();
        logger.info("Started ping thread for worker " + workerId + " on port " + workerPort);
    }

    protected static String generateRandomWorkerId() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}