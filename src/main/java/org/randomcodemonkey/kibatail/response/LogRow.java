package org.randomcodemonkey.kibatail.response;

import java.time.ZonedDateTime;
import org.json.JSONObject;

public class LogRow {

  private String id;

  private ZonedDateTime time;

  private JSONObject source;

  public LogRow(JSONObject item) {
    this.id = item.getString("_id");
    this.source = item.getJSONObject("_source");
    this.time = ZonedDateTime.parse(source.getString("@timestamp"));
    source.remove("@timestamp");
  }

  public String getId() {
    return id;
  }

  public ZonedDateTime getTime() {
    return time;
  }

  public String getField(String field) {
    return internalGetField(source, field);
  }

  private String internalGetField(JSONObject data, String field) {
    int idx = field.indexOf('.');
    if (idx <= 0) {
      return data.has(field) ? data.get(field).toString() : null;
    }
    String key = field.substring(idx + 1);
    String value = field.substring(0, idx);
    JSONObject nestedData = data.optJSONObject(key);
    return nestedData == null ? null : internalGetField(nestedData, value);
  }
}
