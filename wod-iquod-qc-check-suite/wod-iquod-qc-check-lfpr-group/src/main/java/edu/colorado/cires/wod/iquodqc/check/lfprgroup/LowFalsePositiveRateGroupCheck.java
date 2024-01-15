package edu.colorado.cires.wod.iquodqc.check.lfprgroup;

import edu.colorado.cires.wod.iquodqc.check.api.GroupCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import java.util.Collection;
import java.util.List;

public class LowFalsePositiveRateGroupCheck extends GroupCastCheck {

  @Override
  public String getName() {
    return CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return List.of(
        CheckNames.ARGO_IMPOSSIBLE_DATE_TEST.getName(),
        CheckNames.ARGO_IMPOSSIBLE_LOCATION_TEST.getName(),
        CheckNames.WOD_LOOSE_LOCATION_AT_SEA_CHECK.getName(),
        CheckNames.ICDC_AQC_01_LEVEL_ORDER.getName(),
        CheckNames.IQUOD_GROSS_RANGE.getName(),
        CheckNames.WOD_RANGE_CHECK.getName(),
        CheckNames.ICDC_AQC_02_CRUDE_RANGE.getName(),
        CheckNames.EN_BACKGROUND_CHECK.getName(),
        CheckNames.EN_STD_LEV_BKG_AND_BUDDY_CHECK.getName(),
        CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
        CheckNames.ICDC_AQC_05_STUCK_VALUE.getName(),
        CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
        CheckNames.CSIRO_LONG_GRADIENT.getName(),
        CheckNames.EN_STABILITY_CHECK.getName()
    );
  }
}
