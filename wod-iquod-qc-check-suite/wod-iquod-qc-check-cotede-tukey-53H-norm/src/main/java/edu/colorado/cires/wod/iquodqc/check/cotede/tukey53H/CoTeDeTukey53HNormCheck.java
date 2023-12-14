package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import java.util.List;

public class CoTeDeTukey53HNormCheck extends CoTeDeTukey53HCheck {

  private static final double TEMPERATURE_THRESHOLD = 2.5;

  @Override
  public String getName() {
    return "COTEDE_TUKEY_53_NORM_CHECK";
  }

  @Override
  protected List<Integer> checkTukey53H(double[] input, double threshold) {
    return CoTeDeTukey53H.checkTukey53H(input, threshold, true);
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
