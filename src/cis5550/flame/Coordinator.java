package cis5550.flame;

import java.util.*;
import java.net.*;
import java.nio.file.*;
import java.io.*;
import java.lang.reflect.*;

import static cis5550.webserver.Server.*;
import cis5550.kvs.*;
import cis5550.tools.*;

class Coordinator extends cis5550.generic.Coordinator {
  static final Logger logger = Logger.getLogger(Coordinator.class);
  static final String version = "v1.5 Jan 1 2023";
  static int nextJobID = 1;

  public static KVSClient kvs;

  public static void main(String args[]) {
    if (args.length != 2) {
      logger.error("Usage: java cis5550.flame.Coordinator <port> <coordinator_ip:port>");
      System.exit(1);
    }

    int myPort = Integer.valueOf(args[0]);
    kvs = new KVSClient(args[1]);

    logger.info("Flame coordinator (" + version + ") starting on port " + myPort);

    port(myPort);
    registerRoutes();

    get("/", (req, res) -> {
      res.type("text/html");
      return "<html><head><title>Flame coordinator</title></head><body><h1>Flame Coordinator</h1>\n" + workerTable()
          + "</body></html>";
    });

    post("/submit", (req, res) -> {
      String className = req.queryParams("class");
      logger.info("New job submitted; main class is " + className);

      if (className == null) {
        res.status(400, "Bad Request");
        return "Missing class name (parameter 'class')";
      }

      List<String> jobArgs = new ArrayList<>();
      for (int i = 1; req.queryParams("arg" + i) != null; i++)
        jobArgs.add(URLDecoder.decode(req.queryParams("arg" + i), "UTF-8"));

      // upload the JAR to the workers in parallel
      List<String> workers = getWorkers();
      int numWorkers = workers.size();

      Thread threads[] = new Thread[numWorkers];
      String results[] = new String[numWorkers];
      for (int i = 0; i < numWorkers; i++) {
        final int j = i;
        String url = "http://" + workers.get(i) + "/useJAR";
        threads[j] = new Thread(() -> {
          try {
            results[j] = new String(HTTP.doRequest("POST", url, req.bodyAsBytes()).body());
          } catch (Exception e) {
            results[j] = "Exception: " + e;
          }
        });
        threads[j].start();
      }

      // wait for all the uploads to finish
      for (int i = 0; i < threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException ie) {
        }
      }

      // write JAR to local file so it can be run using `invokeRunMethod`
      int id = nextJobID++;
      String jarName = "job-" + id + ".jar";
      Path jarPath = Paths.get(jarName);
      Files.write(jarPath, req.bodyAsBytes());
      File jarFile = jarPath.toFile();

      // find the `run` method in the JAR file and invoke it
      FlameContextImpl context = new FlameContextImpl(jarName);

      try {
        Loader.invokeRunMethod(jarFile, className, context, jobArgs);
      } catch (IllegalAccessException iae) {
        res.status(400, "Bad request");
        return "Double-check that the class " + className
            + " contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
      } catch (NoSuchMethodException iae) {
        res.status(400, "Bad request");
        return "Double-check that the class " + className
            + " contains a public static run(FlameContext, String[]) method";
      } catch (InvocationTargetException ite) {
        logger.error("The job threw an exception, which was:", ite.getCause());
        StringWriter sw = new StringWriter();
        ite.getCause().printStackTrace(new PrintWriter(sw));
        res.status(500, "Job threw an exception");
        return sw.toString();
      }

      String output = context.getOutput();
      if (output.length() > 0) {
        return output;
      } else {
        return "The job succeeded without any output.";
      }
    });

    get("/version", (request, response) -> {
      return "v1.2 Oct 28 2022";
    });
  }
}