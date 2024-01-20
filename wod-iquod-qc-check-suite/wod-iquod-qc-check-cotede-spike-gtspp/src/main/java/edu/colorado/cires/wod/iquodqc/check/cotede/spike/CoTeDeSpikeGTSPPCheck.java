package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

public class CoTeDeSpikeGTSPPCheck extends CoTeDeSpikeCheck {

  private static final double TEMPERATURE_THRESHOLD = 2.;

  @Override
  public String getName() {
    return "COTEDE_SPIKE_GTSPP_CHECK";
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
