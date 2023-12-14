package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

public class CoTeDeGradientGTSPPCheck extends CoTeDeGradientCheck {

  private static final double TEMPERATURE_THRESHOLD = 10.;

  @Override
  public String getName() {
    return "COTEDE_GRADIENT_GTSPP_CHECK";
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
