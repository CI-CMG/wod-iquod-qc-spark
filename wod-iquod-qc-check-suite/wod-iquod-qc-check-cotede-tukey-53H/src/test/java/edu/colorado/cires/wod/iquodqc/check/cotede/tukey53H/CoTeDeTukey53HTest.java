package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;


public class CoTeDeTukey53HTest {
  private static final double[] VALUES = {Double.NaN, 25.34, 25.34, 25.31, 24.99, 230.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  private static final double[] COMPUTE_VALUES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  private static final double[] EXPECTED_NORM_VALUES = {
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      0.04145785,
      0.00274101,
      0.07846155,
      -0.045912,
      0.0599597,
      -0.03974471,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
  };
  
  private static final double[] EXPECTED_VALUES = {
  Double.NaN,
  Double.NaN,
  Double.NaN,
  Double.NaN,
      0.3025,
      0.02,
      0.5725,
      -0.335,
      0.4375,
      -0.29,
  Double.NaN,
  Double.NaN,
  Double.NaN,
  Double.NaN,
  Double.NaN,
};

  private static final List<Integer> EXPECTED_FLAGS = List.of(0, 5, 14);
  
  private static final double THRESHOLD = 6D;
  
  @Test void testTukey53HFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeTukey53H.checkTukey53H(VALUES, THRESHOLD, false));
  }

  @Test void testTukey53HNormFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeTukey53H.checkTukey53H(VALUES, THRESHOLD, true));
  }

  @Test void testComputeTukey53H() {
    assertArrayEquals(EXPECTED_VALUES, CoTeDeTukey53H.computeTukey53H(COMPUTE_VALUES, false), 1E-7);
  }

  @Test void testComputeTukey53HNorm() {
    assertArrayEquals(EXPECTED_NORM_VALUES, CoTeDeTukey53H.computeTukey53H(COMPUTE_VALUES, true), 1E-7);
  }
  
  @Test void testComputeTukey53HNormNans() {
    double[] nanArray = new double[COMPUTE_VALUES.length];
    Arrays.fill(nanArray, Double.NaN);
    assertArrayEquals(nanArray, CoTeDeTukey53H.computeTukey53H(nanArray, true));
  }
}
