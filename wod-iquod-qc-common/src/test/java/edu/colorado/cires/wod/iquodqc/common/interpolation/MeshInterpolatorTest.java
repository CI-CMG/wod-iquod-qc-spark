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

  @Test
  public void test3Values() throws Exception {
    double[] x = new double[] {27.5, 27.5, 32.5};
    double[] y = new double[] {-82.5, -77.5, -77.5};
    double[] values = new double[] {27.71489759, 27.91508028, 26.48958114};

    MeshInterpolator interpolator = new MeshInterpolator(x, y, values);
    double interpolatedValue = interpolator.interpolate(29.6, -78.1333);
    assertEquals(27.291015500316306, interpolatedValue, 0.00000001);
  }
}