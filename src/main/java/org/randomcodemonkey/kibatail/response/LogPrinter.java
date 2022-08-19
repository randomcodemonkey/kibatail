package org.randomcodemonkey.kibatail.response;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogPrinter {

  private static final String FALLBACK_FIELDS = "log_level,logger_name,message";

  private static final String SEPARATOR_FIELD = "-";
  private static final String SEPARATOR_VALUE = "-";

  private List<String> fields;

  public LogPrinter(String fields) {
    if (fields == null || fields.isBlank()) {
      fields = FALLBACK_FIELDS;
    }
    this.fields = new ArrayList<>();
    for (String field : fields.split(",")) {
      this.fields.add(field);
    }
  }

  /**
   * Print the configured fields out of the given response, ignoring any fields the response does
   * not contain
   *
   * @param response Response to print
   */
  public void print(LogResponse response) {
    StringBuilder sb = new StringBuilder();
    response.getRows().stream().forEach(row -> collect(sb, row));
    if (sb.length() == 0) {
      return;
    }
    System.out.print(sb.toString());
  }

  private void collect(StringBuilder result, LogRow row) {
    List<String> values =
        fields.stream()
            .map(
                f -> {
                  if (SEPARATOR_FIELD.equals(f)) {
                    return SEPARATOR_VALUE;
                  }
                  return row.getField(f);
                })
            .filter(v -> v != null)
            .collect(Collectors.toList());
    if (values.isEmpty()) {
      return;
    }
    result.append(row.getTime().toString());
    values.stream().forEach(v -> result.append(' ').append(v));
    result.append('\n');
  }
}
