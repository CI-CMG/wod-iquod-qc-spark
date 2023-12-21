package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.util.OptionalDouble;
import java.util.OptionalInt;

public class Woa {

  private final double mean;
  private final double standardDeviation;
  private final double numberOfObservations;
  private final double standardError;

  public Woa(double mean, double standardDeviation, double numberOfObservations, double standardError) {
    this.mean = mean;
    this.standardDeviation = standardDeviation;
    this.numberOfObservations = numberOfObservations;
    this.standardError = standardError;
  }

  public OptionalDouble getMean() {
    return Double.isNaN(mean) ? OptionalDouble.empty() : OptionalDouble.of(mean);
  }

  public OptionalDouble getStandardDeviation() {
    return Double.isNaN(standardDeviation) ? OptionalDouble.empty() : OptionalDouble.of(standardDeviation);
  }

  public OptionalInt getNumberOfObservations() {
    return Double.isNaN(numberOfObservations) ? OptionalInt.empty() : OptionalInt.of((int) numberOfObservations);
  }

  public OptionalDouble getStandardError() {
    return Double.isNaN(standardError) ? OptionalDouble.empty() : OptionalDouble.of(standardError);
  }
}
