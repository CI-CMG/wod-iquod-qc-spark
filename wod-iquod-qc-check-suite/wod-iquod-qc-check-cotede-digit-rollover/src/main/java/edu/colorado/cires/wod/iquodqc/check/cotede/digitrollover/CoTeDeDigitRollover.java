package edu.colorado.cires.wod.iquodqc.check.cotede.digitrollover;

public class CoTeDeDigitRollover {

  public static boolean checkDigitRollover(double current, double previous, double threshold) {
    double diff = Math.abs(current - previous);
    return !Double.isNaN(diff) && Double.isFinite(diff) && diff <= threshold; 
  }

}
