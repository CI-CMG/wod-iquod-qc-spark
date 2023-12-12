package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

public class CoTeDeGradientGTSPPCheck extends CoTeDeGradientCheck {

  private static final double TEMPERATURE_THRESHOLD = 10.;
  private static final double PRESSURE_THRESHOLD = 10.;
  private static final double SALINITY_THRESHOLD = 10.;

  @Override
  public String getName() {
    return "COTEDE_GRADIENT_GTSPP_CHECK";
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }

  @Override
  protected double getPressureThreshold() {
    return PRESSURE_THRESHOLD;
  }

  @Override
  protected double getSalinityThreshold() {
    return SALINITY_THRESHOLD;
  }
}
