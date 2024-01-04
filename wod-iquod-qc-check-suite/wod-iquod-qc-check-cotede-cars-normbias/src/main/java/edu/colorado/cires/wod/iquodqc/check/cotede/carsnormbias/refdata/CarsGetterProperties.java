package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.StatsGetterProperties;

public class CarsGetterProperties extends StatsGetterProperties {
  
  private final String addOffset;
  private final String scaleFactor;

  public CarsGetterProperties(String mean, String standardDeviation, String latitude,
      String longitude, String depth, String addOffset, String scaleFactor) {
    super(mean, standardDeviation, latitude, longitude, depth);
    this.addOffset = addOffset;
    this.scaleFactor = scaleFactor;
  }

  public String getAddOffset() {
    return addOffset;
  }

  public String getScaleFactor() {
    return scaleFactor;
  }
}
