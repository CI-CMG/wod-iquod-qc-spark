package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

public class StatsGetterProperties {
  private final String mean;
  private final String standardDeviation;
  private final String latitude;
  private final String longitude;
  private final String depth;

  public StatsGetterProperties(String mean, String standardDeviation, String latitude,
      String longitude, String depth) {
    this.mean = mean;
    this.standardDeviation = standardDeviation;
    this.latitude = latitude;
    this.longitude = longitude;
    this.depth = depth;
  }

  public String getMean() {
    return mean;
  }

  public String getStandardDeviation() {
    return standardDeviation;
  }

  public String getLatitude() {
    return latitude;
  }

  public String getLongitude() {
    return longitude;
  }

  public String getDepth() {
    return depth;
  }
}
