package cis5550.webserver;

import java.io.*;
import java.util.*;

class ResponseImpl implements Response {
    private OutputStream out;
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private String contentType = "text/html";
    private byte[] body = null;
    private Map<String, List<String>> headers = new HashMap<>();
    private boolean committed = false;
    private String newSessionId = null; // Track if we need to set a session cookie

    public ResponseImpl(OutputStream out) {
        this.out = out;
    }

    @Override
    public void body(String body) {
        this.body = body.getBytes();
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        this.body = bodyArg;
    }

    @Override
    public void header(String name, String value) {
        if (committed) {
            return;
        }
        if (!headers.containsKey(name)) {
            headers.put(name, new ArrayList<>());
        }
        headers.get(name).add(value);
    }

    @Override
    public void type(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (committed) {
            return;
        }
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!committed) {
            // First call to write() - commit the response
            committed = true;

            // Write status line
            PrintWriter writer = new PrintWriter(out, false);
            writer.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");

            // Write Content-Type
            writer.print("Content-Type: " + contentType + "\r\n");

            // Write Set-Cookie for new session if needed
            if (newSessionId != null) {
                writer.print("Set-Cookie: SessionID=" + newSessionId + "\r\n");
            }

            // Write other headers
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                for (String value : entry.getValue()) {
                    writer.print(entry.getKey() + ": " + value + "\r\n");
                }
            }

            // Add Connection: close header
            writer.print("Connection: close\r\n");

            // End of headers
            writer.print("\r\n");
            writer.flush();
        }

        // Write the body bytes
        out.write(b);
        out.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        // Extra credit - not implemented
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // Extra credit - not implemented
    }

    // Helper methods for Server to access response data
    public byte[] getBody() {
        return body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public boolean hasCommitted() {
        return committed;
    }

    // Method to set the session ID for Set-Cookie header
    public void setNewSessionId(String sessionId) {
        this.newSessionId = sessionId;
    }

    public String getNewSessionId() {
        return newSessionId;
    }
}
