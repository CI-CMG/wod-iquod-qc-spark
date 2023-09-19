package edu.colorado.cires.wod.iquodqc.check.en.spikeandstep;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepChecker;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;

public class EnSpikeAndStepCheck extends CommonCastCheck {


  @Override
  public String getName() {
    return CheckNames.EN_SPIKE_AND_STEP_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return EnSpikeAndStepChecker.getFailedDepths(cast, false);
  }

}
