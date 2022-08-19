package org.randomcodemonkey.kibatail.response;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.json.JSONObject;

public class LogResponse {

  private int statusCode;

  private String error;

  private List<LogRow> rows = new ArrayList<>();

  public Collection<LogRow> getRows() {
    return rows;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getError() {
    return error;
  }

  public Optional<ZonedDateTime> getLastResponseTime() {
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(rows.get(rows.size() - 1).getTime());
  }

  public static LogResponse ok(JSONObject data, ZonedDateTime endOfPreviousRequest) {
    LogResponse r = new LogResponse();

    r.statusCode = HttpStatus.SC_OK;
    JSONObject hits = data.optJSONObject("hits");
    if (hits == null) {
      return r;
    }
    if (hits.optInt("total", 0) == 0) {
      return r;
    }

    for (int i = hits.getJSONArray("hits").length() - 1; i >= 0; i--) {
      LogRow row = new LogRow(hits.getJSONArray("hits").getJSONObject(i));
      if (endOfPreviousRequest == null || row.getTime().isAfter(endOfPreviousRequest)) {
        r.rows.add(row);
      }
    }
    return r;
  }

  public static LogResponse error(int statusCode, String error) {
    LogResponse r = new LogResponse();
    r.statusCode = statusCode;
    r.error = error;
    return r;
  }
}
