package org.randomcodemonkey.kibatail.request;

import org.json.JSONObject;

public interface JSONSerializable {

  /**
   * Serialize this object into a JSONObject
   *
   * @return JSONObject representation of this object
   */
  public JSONObject toJson();
}
