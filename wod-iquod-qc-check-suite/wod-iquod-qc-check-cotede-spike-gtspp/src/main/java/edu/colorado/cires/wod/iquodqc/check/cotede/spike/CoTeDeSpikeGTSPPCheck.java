package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

public class CoTeDeSpikeGTSPPCheck extends CoTeDeSpikeCheck {

  private static final double TEMPERATURE_THRESHOLD = 2.;

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
