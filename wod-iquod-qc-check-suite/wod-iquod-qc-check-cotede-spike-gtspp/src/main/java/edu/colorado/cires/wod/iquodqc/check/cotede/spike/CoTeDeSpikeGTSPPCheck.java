package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import edu.colorado.cires.wod.iquodqc.common.CheckNames;

public class CoTeDeSpikeGTSPPCheck extends CoTeDeSpikeCheck {

  @Override
  public String getName() {
    return CheckNames.COTEDE_GTSPP_SPIKE_CHECK.getName();
  }

  private static final double TEMPERATURE_THRESHOLD = 2.;

  @Override
  public String getName() {
    return CheckNames.COTEDE_SPIKE_GTSPP_CHECK.getName();
  }

  @Override
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
