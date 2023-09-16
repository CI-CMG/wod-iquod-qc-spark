package edu.colorado.cires.wod.iquodqc.check.en.spikeandstep;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepChecker;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;

public class EnSpikeAndStepCheck extends CommonCastCheck {

  private static final String NAME = "EN_spike_and_step_check";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return EnSpikeAndStepChecker.getFailedDepths(cast, false);
  }

}
