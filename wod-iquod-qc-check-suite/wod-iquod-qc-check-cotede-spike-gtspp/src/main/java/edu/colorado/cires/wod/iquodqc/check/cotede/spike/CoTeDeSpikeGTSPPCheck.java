package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

public class CoTeDeSpikeGTSPPCheck extends CoTeDeSpikeCheck {

  private static final double TEMPERATURE_THRESHOLD = 2.;
  private static final double PRESSURE_THRESHOLD = 2.;
  private static final double SALINITY_THRESHOLD = 2.;

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
