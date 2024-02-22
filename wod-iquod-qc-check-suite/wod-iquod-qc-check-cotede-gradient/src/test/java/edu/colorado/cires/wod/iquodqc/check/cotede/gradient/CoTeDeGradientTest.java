package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeGradientTest {

  private static final double[] VALUES = {25.32, Double.NaN, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final List<Integer> FAILED_FLAGS = List.of(10, 11);
  private static final double THRESHOLD = 1.5;

  private static final double[] EXPECTED_GRADIENT = {Double.NaN,  0.01,   0.015,  0.145,  0.605,  0.04,   1.145, -0.67,   0.875, -0.08, -2.575,  1.61,  -0.045,    Double.NaN,    Double.NaN};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  
  @Test void testGradientFlags() {
    assertEquals(FAILED_FLAGS, CoTeDeGradient.getFlags(VALUES, CoTeDeGradient.computeGradient(VALUES), THRESHOLD));
  }
  
  @Test void testComputeGradient() {
    assertArrayEquals(EXPECTED_GRADIENT, CoTeDeGradient.computeGradient(TEMPERATURES), 1e-14);
  }

}
