package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.util.OptionalDouble;

public class Stats {

  private final double mean;
  private final double standardDeviation;

  public Stats(double mean, double standardDeviation) {
    this.mean = mean;
    this.standardDeviation = standardDeviation;
  }

  public OptionalDouble getMean() {
    return Double.isNaN(mean) ? OptionalDouble.empty() : OptionalDouble.of(mean);
  }

  public OptionalDouble getStandardDeviation() {
    return Double.isNaN(standardDeviation) ? OptionalDouble.empty() : OptionalDouble.of(standardDeviation);
  }
  
  public double getNormBias(double temperature) {
    OptionalDouble maybeMean = getMean();
    OptionalDouble maybeStandardDeviation = getStandardDeviation();
    
    if (maybeMean.isEmpty() || maybeStandardDeviation.isEmpty()) {
      return Double.NaN;
    }
    
    return (temperature - maybeMean.getAsDouble()) / maybeStandardDeviation.getAsDouble();
  }
}
