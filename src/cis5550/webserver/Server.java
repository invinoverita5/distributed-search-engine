package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import java.security.*;
import cis5550.tools.Logger;

public class Server extends Thread {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static final int NUM_WORKERS = 200;
    private static final int SOCKET_READ_TIMEOUT_MS = 60000;
    private static Server instance = null;
    private static boolean launched = false;

    private int portNumber = 80;
    private Integer securePortNumber = null;
    private String rootDirectory = null;
    private List<RouteEntry> routes = new ArrayList<>();
    private BlockingQueue<Socket> connectionQueue;

    // Session management
    private Map<String, SessionImpl> sessions = new ConcurrentHashMap<>();
    private static final String SESSION_COOKIE_NAME = "SessionID";
    private static final int SESSION_ID_LENGTH = 20; // 20 chars from 64-char alphabet = 120 bits
    private static final String SESSION_ID_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    private Thread sessionCleanupThread = null;

    // Inner class to store route information
    private static class RouteEntry {
        String method;
        String pathPattern;
        Route handler;

        RouteEntry(String method, String pathPattern, Route handler) {
            this.method = method;
            this.pathPattern = pathPattern;
            this.handler = handler;
        }
    }

    // Static inner class for static files
    public static class staticFiles {
        public static void location(String path) {
            ensureInstance();
            instance.rootDirectory = path;
        }
    }

    // Static methods for API
    public static void port(int port) {
        ensureInstance();
        instance.portNumber = port;
    }

    public static void securePort(int port) {
        ensureInstance();
        instance.securePortNumber = port;
        logger.info("Secure port set to: " + port);
    }

    public static void get(String path, Route handler) {
        ensureInstance();
        instance.routes.add(new RouteEntry("GET", path, handler));
        launchIfNeeded();
    }

    public static void post(String path, Route handler) {
        ensureInstance();
        instance.routes.add(new RouteEntry("POST", path, handler));
        launchIfNeeded();
    }

    public static void put(String path, Route handler) {
        ensureInstance();
        instance.routes.add(new RouteEntry("PUT", path, handler));
        launchIfNeeded();
    }

    private static void ensureInstance() {
        if (instance == null) {
            instance = new Server();
        }
    }

    private static void launchIfNeeded() {
        if (!launched) {
            launched = true;
            instance.start();
        }
    }

    @Override
    public void run() {
        logger.info("Starting server on port " + portNumber +
                   (securePortNumber != null ? " and secure port " + securePortNumber : ""));

        try {
            connectionQueue = new LinkedBlockingQueue<>();

            // Start worker threads
            for (int i = 0; i < NUM_WORKERS; i++) {
                Thread workerThread = new WorkerThread(connectionQueue);
                workerThread.setName("Worker-" + i);
                workerThread.start();
            }

            // Start session cleanup thread
            startSessionCleanupThread();

            logger.info("Server started with " + NUM_WORKERS + " worker threads");

            // Start HTTP server thread
            Thread httpThread = new Thread(() -> serverLoop(portNumber, false));
            httpThread.setName("HTTP-Acceptor");
            httpThread.start();

            // Start HTTPS server thread if secure port is set
            if (securePortNumber != null) {
                Thread httpsThread = new Thread(() -> serverLoop(securePortNumber, true));
                httpsThread.setName("HTTPS-Acceptor");
                httpsThread.start();
            }

            // Wait for threads to complete (they won't unless there's an error)
            httpThread.join();

        } catch (Exception e) {
            logger.fatal("Failed to start server", e);
        }
    }

    private void serverLoop(int port, boolean isSecure) {
        try {
            ServerSocket serverSocket;

            if (isSecure) {
                String pwd = "secret";
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, pwd.toCharArray());
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
                serverSocket = factory.createServerSocket(port);
                logger.info("HTTPS server socket created on port " + port);
            } else {
                serverSocket = new ServerSocket(port);
                logger.info("HTTP server socket created on port " + port);
            }

            // Accept connections
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(SOCKET_READ_TIMEOUT_MS);
                    logger.info("Incoming connection from " + clientSocket.getRemoteSocketAddress() +
                               " on " + (isSecure ? "HTTPS" : "HTTP"));

                    int queueSize = connectionQueue.size();
                    if (queueSize > 50) {
                        logger.warn("Connection queue backlog: " + queueSize + " pending connections");
                    }

                    connectionQueue.put(clientSocket);
                } catch (Exception e) {
                    logger.error("Error accepting connection", e);
                }
            }
        } catch (Exception e) {
            logger.fatal("Failed to start " + (isSecure ? "HTTPS" : "HTTP") + " server on port " + port, e);
        }
    }

    // Session management methods
    private void startSessionCleanupThread() {
        sessionCleanupThread = new Thread(() -> {
            logger.info("Session cleanup thread started");
            while (true) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds
                    cleanupExpiredSessions();
                } catch (InterruptedException e) {
                    logger.info("Session cleanup thread interrupted");
                    break;
                }
            }
        });
        sessionCleanupThread.setName("Session-Cleanup");
        sessionCleanupThread.setDaemon(true);
        sessionCleanupThread.start();
    }

    private void cleanupExpiredSessions() {
        List<String> expiredSessions = new ArrayList<>();
        for (Map.Entry<String, SessionImpl> entry : sessions.entrySet()) {
            if (entry.getValue().isExpired()) {
                expiredSessions.add(entry.getKey());
            }
        }

        for (String sessionId : expiredSessions) {
            sessions.remove(sessionId);
            logger.debug("Removed expired session: " + sessionId);
        }

        if (expiredSessions.size() > 0) {
            logger.info("Cleaned up " + expiredSessions.size() + " expired session(s)");
        }
    }

    private String generateSessionId() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(SESSION_ID_LENGTH);
        for (int i = 0; i < SESSION_ID_LENGTH; i++) {
            sb.append(SESSION_ID_CHARS.charAt(random.nextInt(SESSION_ID_CHARS.length())));
        }
        return sb.toString();
    }

    public synchronized SessionImpl getOrCreateSession(RequestImpl request) {
        // Check if there's a Cookie header with SessionID
        String cookieHeader = request.headers("cookie");
        String existingSessionId = null;

        if (cookieHeader != null) {
            existingSessionId = parseCookieForSessionId(cookieHeader);
            logger.debug("Found cookie header: " + cookieHeader + ", SessionID: " + existingSessionId);
        }

        // If we found a session ID, look it up
        if (existingSessionId != null) {
            SessionImpl session = sessions.get(existingSessionId);
            if (session != null && session.isValid() && !session.isExpired()) {
                // Update last accessed time
                session.updateLastAccessedTime();
                logger.debug("Reusing existing session: " + existingSessionId);
                return session;
            } else if (session != null) {
                logger.debug("Session " + existingSessionId + " is expired or invalid");
            }
        }

        // Create a new session
        String newSessionId = generateSessionId();
        SessionImpl newSession = new SessionImpl(newSessionId, this);
        sessions.put(newSessionId, newSession);

        // Mark the response to include Set-Cookie header
        // We'll handle this in the request handler
        logger.info("Created new session: " + newSessionId);

        return newSession;
    }

    private String parseCookieForSessionId(String cookieHeader) {
        // Parse Cookie header which can contain multiple cookies separated by "; "
        String[] cookies = cookieHeader.split(";\\s*");
        for (String cookie : cookies) {
            String[] parts = cookie.split("=", 2);
            if (parts.length == 2 && parts[0].trim().equals(SESSION_COOKIE_NAME)) {
                return parts[1].trim();
            }
        }
        return null;
    }

    public void removeSession(String sessionId) {
        sessions.remove(sessionId);
        logger.info("Removed session: " + sessionId);
    }

    class WorkerThread extends Thread {
        private final BlockingQueue<Socket> connectionQueue;

        public WorkerThread(BlockingQueue<Socket> queue) {
            this.connectionQueue = queue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket clientSocket = connectionQueue.take();
                    handleConnection(clientSocket);
                } catch (InterruptedException e) {
                    logger.error("Worker thread interrupted", e);
                    break;
                } catch (Exception e) {
                    logger.error("Error in worker thread", e);
                }
            }
        }

        private void handleConnection(Socket socket) {
            try {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();

                while (!socket.isClosed()) {
                    try {
                        // Read headers
                        ByteArrayOutputStream headerBuffer = new ByteArrayOutputStream();
                        int matchPtr = 0;
                        int bytesRead = 0;

                        while (matchPtr < 4) {
                            int b = in.read();
                            if (b == -1) {
                                if (bytesRead == 0) {
                                    socket.close();
                                    return;
                                }
                                break;
                            }

                            bytesRead++;
                            headerBuffer.write(b);

                            if ((matchPtr == 0 || matchPtr == 2) && b == '\r') {
                                matchPtr++;
                            } else if ((matchPtr == 1 || matchPtr == 3) && b == '\n') {
                                matchPtr++;
                            } else {
                                matchPtr = 0;
                            }
                        }

                        if (matchPtr < 4) {
                            break;
                        }

                        // Parse headers
                        byte[] headerBytes = headerBuffer.toByteArray();
                        BufferedReader reader = new BufferedReader(
                            new InputStreamReader(new ByteArrayInputStream(headerBytes))
                        );

                        String requestLine = reader.readLine();
                        if (requestLine == null || requestLine.trim().isEmpty()) {
                            sendErrorResponse(out, 400, "Bad Request");
                            continue;
                        }

                        logger.debug("Request line: " + requestLine);

                        String[] requestParts = requestLine.split(" ");
                        if (requestParts.length != 3) {
                            sendErrorResponse(out, 400, "Bad Request");
                            continue;
                        }

                        String method = requestParts[0];
                        String fullUrl = requestParts[1];
                        String protocol = requestParts[2];

                        // Remove query string from URL for routing
                        String url = fullUrl;
                        String queryString = null;
                        int queryIndex = fullUrl.indexOf('?');
                        if (queryIndex != -1) {
                            url = fullUrl.substring(0, queryIndex);
                            queryString = fullUrl.substring(queryIndex + 1);
                        }

                        // Read headers
                        Map<String, String> headers = new HashMap<>();
                        String headerLine;
                        while ((headerLine = reader.readLine()) != null && !headerLine.isEmpty()) {
                            int colonIndex = headerLine.indexOf(':');
                            if (colonIndex > 0) {
                                String headerName = headerLine.substring(0, colonIndex).trim().toLowerCase();
                                String headerValue = headerLine.substring(colonIndex + 1).trim();
                                headers.put(headerName, headerValue);
                            }
                        }

                        // Read body if present
                        byte[] body = new byte[0];
                        if (headers.containsKey("content-length")) {
                            int contentLength = Integer.parseInt(headers.get("content-length"));
                            body = new byte[contentLength];
                            int totalRead = 0;
                            while (totalRead < contentLength) {
                                int read = in.read(body, totalRead, contentLength - totalRead);
                                if (read == -1) break;
                                totalRead += read;
                            }
                        }

                        // Parse query parameters
                        Map<String, String> queryParams = new HashMap<>();
                        if (queryString != null) {
                            parseQueryParams(queryString, queryParams);
                        }

                        // Parse query params from body if form-encoded
                        if (headers.containsKey("content-type") &&
                            headers.get("content-type").equals("application/x-www-form-urlencoded")) {
                            String bodyString = new String(body, "UTF-8");
                            parseQueryParams(bodyString, queryParams);
                        }

                        // Try to match a route
                        InetSocketAddress remoteAddr = (InetSocketAddress) socket.getRemoteSocketAddress();
                        RouteEntry matchedRoute = null;
                        Map<String, String> pathParams = null;

                        for (RouteEntry route : routes) {
                            if (route.method.equals(method)) {
                                Map<String, String> params = matchPath(route.pathPattern, url);
                                if (params != null) {
                                    matchedRoute = route;
                                    pathParams = params;
                                    break;
                                }
                            }
                        }

                        if (matchedRoute != null) {
                            // Handle dynamic request
                            boolean shouldClose = handleDynamicRequest(out, matchedRoute, method, url, protocol,
                                               headers, queryParams, pathParams, remoteAddr, body);
                            if (shouldClose) {
                                break;
                            }
                        } else {
                            // Try static file
                            handleStaticFile(out, method, url, protocol, headers);
                        }

                    } catch (SocketTimeoutException e) {
                        logger.warn("Socket read timeout after " + SOCKET_READ_TIMEOUT_MS + "ms");
                        break;
                    } catch (SocketException e) {
                        logger.debug("Connection closed by client");
                        break;
                    } catch (Exception e) {
                        logger.error("Error handling request", e);
                        try {
                            sendErrorResponse(out, 400, "Bad Request");
                        } catch (Exception ex) {
                            // Ignore errors when sending error response
                        }
                        break;
                    }
                }

            } catch (Exception e) {
                logger.error("Error handling connection", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error("Error closing socket", e);
                }
            }
        }

        private void parseQueryParams(String queryString, Map<String, String> queryParams) {
            if (queryString == null || queryString.isEmpty()) {
                return;
            }

            String[] pairs = queryString.split("&");
            for (String pair : pairs) {
                int eqIndex = pair.indexOf('=');
                if (eqIndex > 0) {
                    try {
                        String key = URLDecoder.decode(pair.substring(0, eqIndex), "UTF-8");
                        String value = URLDecoder.decode(pair.substring(eqIndex + 1), "UTF-8");

                        // If key already exists, concatenate with comma
                        if (queryParams.containsKey(key)) {
                            queryParams.put(key, queryParams.get(key) + "," + value);
                        } else {
                            queryParams.put(key, value);
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private Map<String, String> matchPath(String pattern, String url) {
            String[] patternParts = pattern.split("/");
            String[] urlParts = url.split("/");

            if (patternParts.length != urlParts.length) {
                return null;
            }

            Map<String, String> params = new HashMap<>();
            for (int i = 0; i < patternParts.length; i++) {
                if (patternParts[i].startsWith(":")) {
                    // Named parameter
                    String paramName = patternParts[i].substring(1);
                    params.put(paramName, urlParts[i]);
                } else if (!patternParts[i].equals(urlParts[i])) {
                    // Literal part doesn't match
                    return null;
                }
            }

            return params;
        }

        private boolean handleDynamicRequest(OutputStream out, RouteEntry route, String method,
                                         String url, String protocol, Map<String, String> headers,
                                         Map<String, String> queryParams, Map<String, String> pathParams,
                                         InetSocketAddress remoteAddr, byte[] body) {
            try {
                RequestImpl request = new RequestImpl(method, url, protocol, headers,
                                                     queryParams, pathParams, remoteAddr, body, instance);
                ResponseImpl response = new ResponseImpl(out);

                Object result = null;
                Exception routeException = null;

                try {
                    result = route.handler.handle(request, response);
                } catch (Exception e) {
                    routeException = e;
                }

                // Check if a new session was created and needs a Set-Cookie header
                if (request.currentSession != null) {
                    // Check if this is a new session (not in the original cookie)
                    String cookieHeader = request.headers("cookie");
                    String existingSessionId = null;
                    if (cookieHeader != null) {
                        existingSessionId = parseCookieForSessionId(cookieHeader);
                    }

                    // If the session ID is different from the cookie, it's a new session
                    if (existingSessionId == null || !existingSessionId.equals(request.currentSession.id())) {
                        response.setNewSessionId(request.currentSession.id());
                        logger.debug("Will set Set-Cookie header for new session: " + request.currentSession.id());
                    }
                }

                // If write() was called, we're done and should close connection
                if (response.hasCommitted()) {
                    return true;
                }

                // If exception occurred, return 500
                if (routeException != null) {
                    logger.error("Exception in route handler", routeException);
                    sendErrorResponse(out, 500, "Internal Server Error");
                    return false;
                }

                // Send response
                byte[] responseBody = null;
                if (result != null) {
                    responseBody = result.toString().getBytes("UTF-8");
                } else if (response.getBody() != null) {
                    responseBody = response.getBody();
                }

                // Write status line
                PrintWriter writer = new PrintWriter(out, false);
                writer.print(protocol + " " + response.getStatusCode() + " " +
                           response.getReasonPhrase() + "\r\n");

                // Write headers
                int contentLength = (responseBody != null) ? responseBody.length : 0;
                writer.print("Content-Length: " + contentLength + "\r\n");
                writer.print("Content-Type: " + response.getContentType() + "\r\n");

                // Write Set-Cookie header if we created a new session
                if (response.getNewSessionId() != null) {
                    writer.print("Set-Cookie: " + SESSION_COOKIE_NAME + "=" + response.getNewSessionId() + "\r\n");
                    logger.debug("Sent Set-Cookie header: " + SESSION_COOKIE_NAME + "=" + response.getNewSessionId());
                }

                for (Map.Entry<String, List<String>> entry : response.getHeaders().entrySet()) {
                    for (String value : entry.getValue()) {
                        writer.print(entry.getKey() + ": " + value + "\r\n");
                    }
                }

                writer.print("\r\n");
                writer.flush();

                // Write body
                if (responseBody != null) {
                    out.write(responseBody);
                    out.flush();
                }

                return false;

            } catch (Exception e) {
                logger.error("Error in dynamic request handler", e);
                try {
                    sendErrorResponse(out, 500, "Internal Server Error");
                } catch (Exception ex) {
                    // Ignore
                }
                return false;
            }
        }

        private void handleStaticFile(OutputStream out, String method, String url,
                                     String protocol, Map<String, String> headers) throws IOException {
            if (rootDirectory == null) {
                sendErrorResponse(out, 404, "Not Found");
                return;
            }

            // Validate protocol
            if (!protocol.equals("HTTP/1.1")) {
                sendErrorResponse(out, 505, "HTTP Version Not Supported");
                return;
            }

            // Check method
            if (method.equals("POST") || method.equals("PUT")) {
                sendErrorResponse(out, 405, "Method Not Allowed");
                return;
            } else if (!method.equals("GET") && !method.equals("HEAD")) {
                sendErrorResponse(out, 501, "Not Implemented");
                return;
            }

            // Security check
            if (url.contains("..")) {
                sendErrorResponse(out, 403, "Forbidden");
                return;
            }

            String filePath = rootDirectory + url;
            File file = new File(filePath);

            logger.debug("Requested URL: " + url + ", File path: " + filePath);

            if (!file.exists()) {
                sendErrorResponse(out, 404, "Not Found");
                return;
            }

            if (!file.canRead()) {
                sendErrorResponse(out, 403, "Forbidden");
                return;
            }

            if (!file.isFile()) {
                sendErrorResponse(out, 403, "Forbidden");
                return;
            }

            String contentType = getContentType(filePath);
            byte[] fileContent = Files.readAllBytes(file.toPath());

            PrintWriter writer = new PrintWriter(out, false);
            writer.print("HTTP/1.1 200 OK\r\n");
            writer.print("Content-Type: " + contentType + "\r\n");
            writer.print("Content-Length: " + fileContent.length + "\r\n");
            writer.print("\r\n");
            writer.flush();

            if (method.equals("GET")) {
                out.write(fileContent);
                out.flush();
            }
        }

        private String getContentType(String filePath) {
            if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) {
                return "image/jpeg";
            } else if (filePath.endsWith(".txt")) {
                return "text/plain";
            } else if (filePath.endsWith(".html")) {
                return "text/html";
            } else {
                return "application/octet-stream";
            }
        }

        private void sendErrorResponse(OutputStream out, int statusCode, String statusMessage) throws IOException {
            String body = statusCode + " " + statusMessage;

            PrintWriter writer = new PrintWriter(out, false);
            writer.print("HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n");
            writer.print("Content-Type: text/plain\r\n");
            writer.print("Content-Length: " + body.length() + "\r\n");
            writer.print("\r\n");
            writer.print(body);
            writer.flush();
        }
    }
}
