package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.util.OptionalDouble;
import java.util.OptionalInt;

public class WoaStats extends Stats {
  
  private final double numberOfObservations;
  private final double standardError;

  public WoaStats(double mean, double standardDeviation, double numberOfObservations, double standardError) {
    super(mean, standardDeviation);
    this.numberOfObservations = numberOfObservations;
    this.standardError = standardError;
  }

  public OptionalInt getNumberOfObservations() {
    return Double.isNaN(numberOfObservations) ? OptionalInt.empty() : OptionalInt.of((int) numberOfObservations);
  }

  public OptionalDouble getStandardError() {
    return Double.isNaN(standardError) ? OptionalDouble.empty() : OptionalDouble.of(standardError);
  }
}
