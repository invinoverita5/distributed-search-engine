package cis5550.tools;

import java.util.*;
import java.net.*;
import java.io.*;
import javax.net.ssl.*;
import java.security.*;
import java.security.cert.X509Certificate;

public class HTTP {
  public static class Response {
    byte body[];
    Map<String,String> headers;
    int statusCode;

    public Response(byte bodyArg[], Map<String,String> headersArg, int statusCodeArg) {
      body = bodyArg;
      headers = headersArg;
      statusCode = statusCodeArg;
    }

    public byte[] body() {
      return body;
    }

    public int statusCode() {
      return statusCode;
    }

    public Map<String,String> headers() {
      return headers;
    }
  }

  private static void setupTrustAllCerts() {
    TrustManager[] trustAllCerts = { new X509TrustManager() {
      public X509Certificate[] getAcceptedIssuers() { return null; }
      public void checkClientTrusted(X509Certificate[] certs, String authType) { }
      public void checkServerTrusted(X509Certificate[] certs, String authType) { }
    } };
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
    }
  }

  public static Response doRequest(String method, String urlArg, byte uploadOrNull[]) throws IOException {
    return doRequestWithTimeout(method, urlArg, uploadOrNull, -1, false);
  }

  public static Response doRequestWithTimeout(String method, String urlArg, byte uploadOrNull[], int timeoutMillis, boolean isHeadRequest) throws IOException {
    HttpURLConnection connection = null;
    try {
      URL url;
      try {
        url = new URI(urlArg).toURL();
      } catch (URISyntaxException e) {
        throw new IOException("Invalid URL: " + urlArg, e);
      }

      if (url.getProtocol().equalsIgnoreCase("https")) {
        setupTrustAllCerts();
      }

      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(method);

      if (timeoutMillis > 0) {
        connection.setConnectTimeout(timeoutMillis);
        connection.setReadTimeout(timeoutMillis);
      }

      if (uploadOrNull != null) {
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/octet-stream");
        connection.setRequestProperty("Content-Length", String.valueOf(uploadOrNull.length));
        try (OutputStream out = connection.getOutputStream()) {
          out.write(uploadOrNull);
          out.flush();
        }
      }

      int statusCode = connection.getResponseCode();

      Map<String, String> headers = new HashMap<>();
      for (Map.Entry<String, List<String>> entry : connection.getHeaderFields().entrySet()) {
        if (entry.getKey() != null) {
          headers.put(entry.getKey().toLowerCase(), String.join(", ", entry.getValue()));
        }
      }

      byte[] body = new byte[0];
      if (!isHeadRequest) {
        InputStream in;
        try {
          in = connection.getInputStream();
        } catch (IOException e) {
          in = connection.getErrorStream();
        }

        if (in != null) {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          byte[] buf = new byte[8192];
          int n;
          while ((n = in.read(buf)) != -1) {
            buffer.write(buf, 0, n);
          }
          in.close();
          body = buffer.toByteArray();
        }
      }

      return new Response(body, headers, statusCode);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}