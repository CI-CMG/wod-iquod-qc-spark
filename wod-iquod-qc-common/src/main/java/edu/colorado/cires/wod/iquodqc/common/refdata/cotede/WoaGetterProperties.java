package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

public class WoaGetterProperties extends StatsGetterProperties {
  
  private final String numberOfObservations;
  private final String standardError;

  public WoaGetterProperties(String mean, String standardDeviation, String latitude,
      String longitude, String depth, String numberOfObservations, String standardError) {
    super(mean, standardDeviation, latitude, longitude, depth);
    this.numberOfObservations = numberOfObservations;
    this.standardError = standardError;
  }

  public String getNumberOfObservations() {
    return numberOfObservations;
  }

  public String getStandardError() {
    return standardError;
  }
}
