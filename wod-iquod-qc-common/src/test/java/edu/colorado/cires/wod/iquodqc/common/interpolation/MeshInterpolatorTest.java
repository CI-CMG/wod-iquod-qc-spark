package edu.colorado.cires.wod.iquodqc.common.interpolation;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class MeshInterpolatorTest {

  @Test
  public void test3Simple() throws Exception {
    double[] x = new double[] {0D, 1D, 0D};
    double[] y = new double[] {0D, 1D, 1D};
    double[] values = new double[] {0D, 1D, 1D};

    MeshInterpolator interpolator = new MeshInterpolator(x, y, values);
    double interpolatedValue = interpolator.interpolate(0D, 0.5);
    assertEquals(0.5, interpolatedValue, 0.00001);
  }

}