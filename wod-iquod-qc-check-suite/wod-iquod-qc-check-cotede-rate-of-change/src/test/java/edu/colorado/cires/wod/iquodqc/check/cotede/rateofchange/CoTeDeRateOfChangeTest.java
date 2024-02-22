package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeRateOfChangeTest {

  private static final double[] VALUES = {25.32, 25.34, 25.34, Double.NaN, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final List<Integer> EXPECTED_FLAGS = List.of(9, 10);
  private static final double THRESHOLD = 4;

  private static final double[] EXPECTED_RATE_OF_CHANGE = {Double.NaN, 0.02, 0., -0.03, -0.32, -1.53, -1.61, -3.9, -2.56, -4.31, -4.15, 1., -2.22,
      -2.13, Double.NaN};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  @Test
  void testRateOfChangeFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeRateOfChange.getFlags(VALUES, CoTeDeRateOfChange.computeRateOfChange(VALUES), THRESHOLD));
  }

  @Test
  void testComputeRateOfChange() {
    assertArrayEquals(EXPECTED_RATE_OF_CHANGE, CoTeDeRateOfChange.computeRateOfChange(TEMPERATURES), 1e-14);
  }

}
