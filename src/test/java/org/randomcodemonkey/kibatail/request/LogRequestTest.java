package org.randomcodemonkey.kibatail.request;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.randomcodemonkey.kibatail.request.LogRequest.BooleanFilter;
import org.randomcodemonkey.kibatail.request.LogRequest.PhraseFilterMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogRequestTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogRequestTest.class);

  @Test
  public void testLogRequestJsonSerialization() {
    LOGGER.info("Test LogRequest JSON serialization");
    LogRequest req = new LogRequest(60);
    req.setSize(1000);

    BooleanFilter filter = new BooleanFilter();
    filter.addShould(new PhraseFilterMatch().setPhrase("app").setValue("hello-world"));
    req.addQueryFilter(filter);

    JSONObject json = req.toJson();
    LOGGER.debug("LogRequest JSON:\n{}", json.toString());
    Assertions.assertNotNull(json);
    Assertions.assertTrue(json.getBoolean("version"));
    Assertions.assertEquals(1000, json.getInt("size"));
    Assertions.assertNotNull(json.getJSONArray("sort"));
    Assertions.assertEquals(1, json.getJSONArray("sort").length());
    Assertions.assertNotNull(json.getJSONArray("stored_fields"));
    Assertions.assertNotNull(json.getJSONObject("script_fields"));
    Assertions.assertNotNull(json.getJSONObject("_source"));

    JSONArray jsonFilter = json.getJSONObject("query").getJSONObject("bool").getJSONArray("filter");
    Assertions.assertNotNull(jsonFilter);
    Assertions.assertEquals(2, jsonFilter.length());

    JSONObject rangeFilter = jsonFilter.getJSONObject(0);
    Assertions.assertNotNull(rangeFilter);
    Assertions.assertNotNull(rangeFilter.getJSONObject("range").getJSONObject("@timestamp"));
    Assertions.assertNotNull(
        rangeFilter.getJSONObject("range").getJSONObject("@timestamp").getString("gte"));
    Assertions.assertNotNull(
        rangeFilter.getJSONObject("range").getJSONObject("@timestamp").getString("lte"));

    JSONObject phraseFilter = jsonFilter.getJSONObject(1).getJSONObject("bool");
    Assertions.assertEquals(1, phraseFilter.getInt("minimum_should_match"));
    Assertions.assertNotNull(phraseFilter.getJSONArray("should"));
    Assertions.assertEquals(1, phraseFilter.getJSONArray("should").length());
    JSONObject phraseFilterMatch = phraseFilter.getJSONArray("should").getJSONObject(0);
    Assertions.assertNotNull(phraseFilterMatch);
    Assertions.assertNotNull(phraseFilterMatch.getJSONObject("match_phrase"));
    Assertions.assertNotNull(phraseFilterMatch.getJSONObject("match_phrase").getString("app"));
  }
}
