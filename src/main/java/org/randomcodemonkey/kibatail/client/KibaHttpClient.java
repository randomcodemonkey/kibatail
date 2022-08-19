package org.randomcodemonkey.kibatail.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.randomcodemonkey.kibatail.request.LogRequest;
import org.randomcodemonkey.kibatail.response.LogResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KibaHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(KibaHttpClient.class);

  private URI server;

  private String user;

  private String password;

  private String auth = null;

  private String kbnVersion = null;

  private final LogRequest request;

  private final CloseableHttpClient httpClient;

  public KibaHttpClient(URI server, String indexPattern, int initialLookBackSeconds)
      throws URISyntaxException {
    this.server =
        new URIBuilder(server)
            .setPathSegments("elasticsearch", indexPattern, "_search")
            .addParameter("rest_total_hits_as_int", "true")
            .addParameter("ignore_unavailable", "true")
            .addParameter("ignore_throttled", "true")
            .addParameter("timeout", "30000ms")
            .build();
    this.httpClient = HttpClients.createDefault();
    this.request = new LogRequest(initialLookBackSeconds);
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public LogRequest getRequest() {
    return request;
  }

  public LogResponse executeRequest() throws ClientProtocolException, IOException {
    if (user != null && password != null && auth == null) {
      auth =
          "Basic "
              + Base64.getEncoder()
                  .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    HttpPost post = new HttpPost(server);
    if (auth != null) {
      post.addHeader("Authorization", auth);
    }
    post.addHeader("kbn-version", kbnVersion);
    String requestData = request.toJson().toString();

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Send: {}", requestData);
    }
    LOGGER.info("SEND {}", requestData);
    post.setEntity(new StringEntity(requestData));

    LogResponse logResponse = null;
    try (CloseableHttpResponse response = httpClient.execute(post)) {
      String body = EntityUtils.toString(response.getEntity());
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Received: {}", body);
      }
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        JSONObject json = new JSONObject(body);
        if (isValid(json)) {
          logResponse = LogResponse.ok(json, request.getEndOfPreviousRequest());
        }
      }
      if (logResponse == null) {
        LOGGER.info("Invalid response {}: {}", response.getStatusLine(), body);
        logResponse = LogResponse.error(response.getStatusLine().getStatusCode(), body);
      }
      return logResponse;
    } finally {
      if (logResponse != null) {
        logResponse
            .getLastResponseTime()
            .ifPresentOrElse(c -> request.updateRange(c), () -> request.updateRange());
      } else {
        request.updateRange();
      }
    }
  }

  private boolean isValid(JSONObject json) {
    JSONObject shards = json.optJSONObject("_shards");
    if (shards == null) {
      return true; // debatable
    }
    return shards.optInt("failed") == 0;
  }
}
