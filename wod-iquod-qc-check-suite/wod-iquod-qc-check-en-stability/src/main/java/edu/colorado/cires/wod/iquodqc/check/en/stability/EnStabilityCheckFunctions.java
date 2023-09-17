package edu.colorado.cires.wod.iquodqc.check.en.stability;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class EnStabilityCheckFunctions {

  private static final List<Double> coef = Collections.unmodifiableList(Arrays.asList(
      0D,
      1.067610e-5,
      -1.434297e-6,
      -7.566349e-9,
      -8.535585e-6,
      3.074672e-8,
      1.918639e-8,
      1.788718e-10
  ));

  private static final List<Double> p1CF = Collections.unmodifiableList(Arrays.asList(
      9.99843699e2,
      7.35212840e0,
      -5.45928211e-2,
      3.98476704e-4,
      2.96938239e0,
      -7.23268813e-3,
      2.12382341e-3,
      1.04004591e-2,
      1.03970529e-7,
      5.18761880e-6,
      -3.24041825e-8,
      -1.23869360e-11
  ));

  private static final List<Double> p2CF = Collections.unmodifiableList(Arrays.asList(
      1.0,
      7.28606739e-3,
      -4.60835542e-5,
      3.68390573e-7,
      1.80809186e-10,
      2.14691708e-3,
      -9.27062484e-6,
      -1.78343643e-10,
      4.76534122e-6,
      1.63410736e-9,
      5.30848875e-6,
      -3.03175128e-16,
      -1.27934137e-17
  ));

  static double potentialTemperature(double s, double t, double p) {
    /*
      approximation for potential temperature given in McDougall et al 2003 (http://journals.ametsoc.org/doi/pdf/10.1175/1520-0426%282003%2920%3C730%3AAACEAF%3E2.0.CO%3B2)
      S in psu, T in degrees C, p in db
      note p_r = 0 for these fit values
     */
    double poly = coef.get(1);
    poly += coef.get(2) * s;
    poly += coef.get(3) * p;
    poly += coef.get(4) * t;
    poly += coef.get(5) * s * t;
    poly += coef.get(6) * t * t;
    poly += coef.get(7) * t * p;
    return t + p * poly;
  }


  static double mcdougallEOS(double salinity, double temperature, double pressure) {
    /*
      equation of state defined in McDougall et al 2003 (http://journals.ametsoc.org/doi/pdf/10.1175/1520-0426%282003%2920%3C730%3AAACEAF%3E2.0.CO%3B2)
      returns density in kg/m^3
     */
    double p1 = p1CF.get(0);
    p1 += p1CF.get(1) * temperature;
    p1 += p1CF.get(2) * temperature * temperature;
    p1 += p1CF.get(3) * temperature * temperature * temperature;
    p1 += p1CF.get(4) * salinity;
    p1 += p1CF.get(5) * salinity * temperature;
    p1 += p1CF.get(6) * salinity * salinity;
    p1 += p1CF.get(7) * pressure;
    p1 += p1CF.get(8) * pressure * temperature * temperature;
    p1 += p1CF.get(9) * pressure * salinity;
    p1 += p1CF.get(10) * pressure * pressure;
    p1 += p1CF.get(11) * pressure * pressure * temperature * temperature;

    double p2 = p2CF.get(0);
    p2 += p2CF.get(1) * temperature;
    p2 += p2CF.get(2) * temperature * temperature;
    p2 += p2CF.get(3) * temperature * temperature * temperature;
    p2 += p2CF.get(4) * temperature * temperature * temperature * temperature;
    p2 += p2CF.get(5) * salinity;
    p2 += p2CF.get(6) * salinity * temperature;
    p2 += p2CF.get(7) * salinity * temperature * temperature * temperature;
    p2 += p2CF.get(8) * Math.pow(salinity, 1.5);
    p2 += p2CF.get(9) * Math.pow(salinity, 1.5) * temperature * temperature;
    p2 += p2CF.get(10) * pressure;
    p2 += p2CF.get(11) * pressure * pressure * temperature * temperature * temperature;
    p2 += p2CF.get(12) * pressure * pressure * pressure * temperature;

    return p1 / p2;
  }

  private EnStabilityCheckFunctions() {

  }
}
