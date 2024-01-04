package edu.colorado.cires.wod.iquodqc.check.cotede.constantclustersize;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

public class ConstantClusterSizeTest {
  
  private static final int[] EXPECTED_CLUSTER_SIZE1 = {0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  private static final int[] EXPECTED_CLUSTER_SIZE2 = {3, 3, 3, 3, 0, 0, 2, 2, 2, 0, 0, 0, 1, 1, 0};
  private static final double[] TEMPERATURES1 = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] TEMPERATURES2 = {25.32, 25.34, 25.34, 25.24, 24.99, 23.46, 17.87, 17.95, 17.85, Double.NaN, 6.93, Double.NaN, 5.71, 5.61, Double.NaN};
  private static final double TOLERANCE = 0.1;
  
  @Test void testZeroTolerance() {
    assertArrayEquals(EXPECTED_CLUSTER_SIZE1, ConstantClusterSize.computeClusterSizes(TEMPERATURES1, 0));
  }
  
  @Test void testNonZeroTolerance() {
    assertArrayEquals(EXPECTED_CLUSTER_SIZE2, ConstantClusterSize.computeClusterSizes(TEMPERATURES2, TOLERANCE));
  }

}
