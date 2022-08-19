package org.randomcodemonkey.kibatail.request;

import com.beust.jcommander.internal.Nullable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An object representing a single Kibana API 'log' request.
 *
 * <p>The serialized JSON of this object is POSTed to the kibana search API
 * at <code>/elasticsearch/indexpattern/_search/<code>
 *
 * @author ikaakkola
 */
public class LogRequest implements JSONSerializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogRequest.class);

  private boolean version = true;

  private int size = 500;

  private List<SortDefinition> sort = List.of(new SortDefinition());

  private final BooleanFilter query;

  private int fragmentSize = 2147483647;

  private ZonedDateTime endOfPreviousRequest;

  public LogRequest(int initialLookbackSeconds) {
    query = new BooleanFilter();
    query.updateRange(
        ZonedDateTime.now(ZoneId.systemDefault())
            .minus(initialLookbackSeconds, ChronoUnit.SECONDS));
  }

  @Override
  public JSONObject toJson() {
    JSONObject result = new JSONObject();
    result.put("version", version);
    result.put("size", size);
    result.put("sort", sort.stream().map(item -> item.toJson()).collect(Collectors.toList()));
    result.put("stored_fields", new JSONArray().put("*"));
    result.put("script_fields", new JSONObject());
    result.put("_source", new JSONObject().put("excludes", new JSONArray()));
    result.put("query", query.toJson());
    result.put(
        "highlight",
        new JSONObject()
            .put("fields", new JSONObject().put("*", new JSONObject()))
            .put("fragment_size", fragmentSize));
    return result;
  }

  /**
   * Set the maximum number of items the request should return
   *
   * @param size Maximum number of items to return
   */
  public void setSize(int size) {
    this.size = size;
  }

  /**
   * Add a sort for this request
   *
   * @param sort SortDefinition to add to request
   */
  public void addSort(SortDefinition sort) {
    this.sort.add(sort);
  }

  /**
   * Set the sort for this request
   *
   * @param sort SortDefinition for request
   */
  public void setSort(SortDefinition sort) {
    this.sort.clear();
    this.sort.add(sort);
  }

  /** Update the 'lte' (end, to) time of this request to now */
  public void updateRange() {
    query.updateRange();
  }
  /**
   * Update the request 'gte' (from) and 'lte' (to) times.
   *
   * <p>The from time is set to the given Instant. The to time is set to now.
   *
   * @param endOfPreviousRequest The 'lte' (to) time of the previous request
   * @returns The 'to' time of the request after the update
   */
  public void updateRange(ZonedDateTime endOfPreviousRequest) {
    if (endOfPreviousRequest == null) {
      updateRange();
    } else {
      query.updateRange(endOfPreviousRequest);
    }
    this.endOfPreviousRequest = endOfPreviousRequest;
  }

  /** @return the 'lte' value of the previous request */
  public ZonedDateTime getEndOfPreviousRequest() {
    return endOfPreviousRequest;
  }

  /**
   * Add a filter to the query of this request.
   *
   * @param filter Filter to add
   */
  public void addQueryFilter(QueryFilter filter) {
    query.addFilter(filter);
  }

  public static class SortDefinition implements JSONSerializable {

    private String field = "@timestamp";

    private String order = "desc";

    private String unmappedType = "boolean";

    public SortDefinition setField(String field) {
      this.field = field;
      return this;
    }

    public SortDefinition setOrder(String order) {
      this.order = order;
      return this;
    }

    public SortDefinition setUnmappedType(String unmappedType) {
      this.unmappedType = unmappedType;
      return this;
    }

    @Override
    public JSONObject toJson() {
      return new JSONObject()
          .put(field, new JSONObject().put("order", order).put("unmapped_type", unmappedType));
    }
  }

  public static class BooleanFilter implements QueryFilter {

    private List<FilterMatch> must = new ArrayList<>();

    private List<FilterMatch> mustNot = new ArrayList<>();

    private List<FilterMatch> should = new ArrayList<>();

    private List<QueryFilter> filter = new ArrayList<>();

    private int minimumShouldMatch = 1;

    public BooleanFilter setMinimumShouldMatch(int minimumShouldMatch) {
      this.minimumShouldMatch = minimumShouldMatch;
      return this;
    }

    /**
     * Update the 'lte' (end, to) timestamps of the first nested RangeFilter of this Filter to now,
     * or create a RangeFilter if none is found.
     */
    public void updateRange() {
      updateRange(null);
    }

    /**
     * Update the 'gte' (from, start) and 'lte' (end, to) timestamps of the first nested RangeFilter
     * of this Filter, or create a RangeFilter if none is found.
     *
     * @param start The 'gte' (from, start) time for the request
     */
    public void updateRange(ZonedDateTime start) {

      RangeFilter range =
          filter.stream()
              .filter(possible -> possible instanceof RangeFilter)
              .map(possible -> (RangeFilter) possible)
              .findFirst()
              .orElseGet(
                  () -> {
                    RangeFilter f = new RangeFilter();
                    filter.add(f);
                    return f;
                  });

      range.updateRange(start);
    }

    public BooleanFilter addShould(FilterMatch should) {
      this.should.add(should);
      return this;
    }

    public BooleanFilter addFilter(QueryFilter filter) {
      this.filter.add(filter);
      return this;
    }

    @Override
    public JSONObject toJson() {
      JSONObject r = new JSONObject();
      r.put("should", should.stream().map(item -> item.toJson()).collect(Collectors.toList()));
      if (should.size() > 0) {
        r.put("minimum_should_match", minimumShouldMatch);
      }

      r.put("must", must.stream().map(item -> item.toJson()).collect(Collectors.toList()));
      r.put("must_not", mustNot.stream().map(item -> item.toJson()).collect(Collectors.toList()));
      r.put("filter", filter.stream().map(item -> item.toJson()).collect(Collectors.toList()));

      return new JSONObject().put("bool", r);
    }
  }

  public static class PhraseFilterMatch implements FilterMatch {

    private String phrase;

    private String value;

    public PhraseFilterMatch setPhrase(String phrase) {
      this.phrase = phrase;
      return this;
    }

    public PhraseFilterMatch setValue(String value) {
      this.value = value;
      return this;
    }

    @Override
    public JSONObject toJson() {
      return new JSONObject().put("match_phrase", new JSONObject().putOpt(phrase, value));
    }
  }

  public static class RangeFilter implements QueryFilter {
    private String field = "@timestamp";

    private ZonedDateTime gte;

    private ZonedDateTime lte;

    private String format = "strict_date_optional_time";

    public RangeFilter() {
      this(Instant.now().atZone(ZoneId.systemDefault()).minus(10, ChronoUnit.SECONDS));
    }

    private RangeFilter(ZonedDateTime from) {
      gte = from;
      lte = ZonedDateTime.now(ZoneId.systemDefault());
    }

    public ZonedDateTime updateRange(@Nullable ZonedDateTime start) {
      if (start != null) {
        gte = start;
      }
      lte = ZonedDateTime.now(ZoneId.systemDefault());
      LOGGER.debug("Updated range gte={} lte={}", gte, lte);
      return lte;
    }

    public RangeFilter setField(String field) {
      this.field = field;
      return this;
    }

    public RangeFilter setFormat(String format) {
      this.format = format;
      return this;
    }

    @Override
    public JSONObject toJson() {
      return new JSONObject()
          .put(
              "range",
              new JSONObject()
                  .put(
                      field,
                      new JSONObject()
                          .put("gte", dateFormat(gte))
                          .put("lte", dateFormat(lte))
                          .put("format", format)));
    }

    private String dateFormat(ZonedDateTime time) {
      OffsetDateTime t = time.toOffsetDateTime();
      return t.withNano(0)
          .with(ChronoField.MILLI_OF_SECOND, (Math.round(t.getNano() / 1000 / 1000)))
          .toString();
    }
  }
}
