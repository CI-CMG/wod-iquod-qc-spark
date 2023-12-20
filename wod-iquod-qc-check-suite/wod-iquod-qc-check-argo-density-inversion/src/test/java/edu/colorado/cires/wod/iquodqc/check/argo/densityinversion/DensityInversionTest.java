package edu.colorado.cires.wod.iquodqc.check.argo.densityinversion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import edu.colorado.cires.wod.iquodqc.check.argo.densityinversion.DensityInversion.InsituDensityComputation;
import org.junit.jupiter.api.Test;

public class DensityInversionTest {
  @Test void testDensityStep1() {
    double[] p = {1.0, 100, 200, 300, 500, 5000, Double.NaN};
    double[] t = {27.44, 14.55, 11.96, 11.02, 7.65, 2.12, 2.12};
    double[] s = {35.71, 35.50, 35.13, 35.02, 34.72, 35.03, 35.03};

    double[] output = {Double.NaN, 3.3484632, 0.2433187, 0.0911988, 0.317172, 0.9046589, Double.NaN};

    double[] dRho = densitySteps(s, t, p);
        
    assertArrayEquals(output, dRho, 1E-6);
  }
  
  @Test void testDensityStep2() {
    double[] p = {2, 6, 10, 21, 44, 79, 100, 150,
        200, 400, 410, 650, 1000, 2000, 5000};
    double[] t = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
        15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
    double[] s = {36.49, 36.51, 36.52, 36.53, 36.59, 36.76, 36.81, 36.39,
        35.98, 35.30, 35.28, 34.93, 34.86, Double.NaN, Double.NaN};
    
    double[] output = {Double.NaN, 0.0091339, 0.0077907, 0.0175282, 0.1450310,
        0.5896058, 0.5023247, 0.7156530, 0.2924434, 0.3559480, 0.6476343, -0.4131068,
        0.2489996, Double.NaN, Double.NaN};
    
    double[] dRho = densitySteps(s, t, p);

    assertArrayEquals(output, dRho, 1E-6);
  }

  public static double[] densitySteps(double[] salinity, double[] temperature, double[] pressure) {
    double[] result = new double[salinity.length];

    InsituDensityComputation densityComputation = null;
    for (int i = 0; i < salinity.length; i++) {
      densityComputation = DensityInversion.computeInsituDensity(salinity[i], temperature[i], pressure[i], densityComputation);

      result[i] = densityComputation.getDifference();
    }

    return result;
  }

}
