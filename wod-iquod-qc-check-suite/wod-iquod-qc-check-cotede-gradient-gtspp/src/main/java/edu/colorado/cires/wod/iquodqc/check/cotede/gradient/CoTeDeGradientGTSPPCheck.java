package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import edu.colorado.cires.wod.iquodqc.common.CheckNames;

public class CoTeDeGradientGTSPPCheck extends CoTeDeGradientCheck {

  private static final double TEMPERATURE_THRESHOLD = 10.;

  @Override
  public String getName() {
    return CheckNames.COTEDE_GRADIENT_GTSPP_CHECK.getName();
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
