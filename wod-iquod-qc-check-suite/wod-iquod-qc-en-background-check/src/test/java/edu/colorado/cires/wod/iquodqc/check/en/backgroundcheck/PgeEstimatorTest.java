package edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Test;

public class PgeEstimatorTest {

  @Test
  public void testEnBackgroundCheckEstimatePge() throws Exception {
    assertTrue(Precision.equals(PgeEstimator.estimateProbabilityOfGrossError(1, false), 0.05D));
    assertTrue(Precision.equals(PgeEstimator.estimateProbabilityOfGrossError(16, true), 0.525D));
    assertTrue(Precision.equals(PgeEstimator.estimateProbabilityOfGrossError(4, false), 0.01D));
  }
}