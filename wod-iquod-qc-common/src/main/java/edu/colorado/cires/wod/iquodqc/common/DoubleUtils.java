package edu.colorado.cires.wod.iquodqc.common;

import java.util.Optional;
import org.apache.commons.math3.util.Precision;

public class DoubleUtils {
  public static boolean doubleEquals(Optional<Double> v1, Optional<Double> v2){
    if (v1.isEmpty() || v2.isEmpty()){
      return false;
    }
    return Precision.equals(v1.get(), v2.get(),0.000001d );
  }
  public static boolean lessThanOrEqual(double v1, double v2){
    if (Precision.equals(v1, v2,0.000001d ) || (v1 < v2)){
      return  true;
    }
    return false;
  }

  public static boolean greaterThan(double v1, double v2){
    if ((v1 < v2) || Precision.equals(v1, v2,0.000001d )){
      return  false;
    }
    return true;
  }

  public static boolean lessThan(double v1, double v2){
    if ((v1 > v2) || Precision.equals(v1, v2,0.000001d )){
      return  false;
    }
    return true;
  }
}
