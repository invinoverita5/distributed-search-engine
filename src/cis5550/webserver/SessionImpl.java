package cis5550.webserver;

import java.util.*;
import cis5550.tools.Logger;

class SessionImpl implements Session {
    private static final Logger logger = Logger.getLogger(SessionImpl.class);

    private String sessionId;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval = 300; // Default 300 seconds
    private Map<String, Object> attributes = new HashMap<>();
    private boolean valid = true;
    private Server server;

    public SessionImpl(String id, Server server) {
        this.sessionId = id;
        this.server = server;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        logger.debug("Created new session with ID: " + id);
    }

    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds;
        logger.debug("Set max active interval for session " + sessionId + " to " + seconds + " seconds");
    }

    @Override
    public void invalidate() {
        logger.debug("Invalidating session: " + sessionId);
        valid = false;
        attributes.clear();
        server.removeSession(sessionId);
    }

    @Override
    public Object attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
        logger.debug("Set attribute '" + name + "' in session " + sessionId);
    }

    // Internal methods for session management
    public synchronized boolean isValid() {
        return valid;
    }

    public synchronized boolean isExpired() {
        if (!valid) {
            return true;
        }
        long now = System.currentTimeMillis();
        long elapsed = (now - lastAccessedTime) / 1000; // Convert to seconds
        return elapsed >= maxActiveInterval;
    }

    public synchronized void updateLastAccessedTime() {
        this.lastAccessedTime = System.currentTimeMillis();
    }

    public int getMaxActiveInterval() {
        return maxActiveInterval;
    }
}
