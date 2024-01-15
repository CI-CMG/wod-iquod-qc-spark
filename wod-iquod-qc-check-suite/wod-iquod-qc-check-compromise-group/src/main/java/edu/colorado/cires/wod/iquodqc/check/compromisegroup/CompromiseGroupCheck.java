package edu.colorado.cires.wod.iquodqc.check.compromisegroup;

import edu.colorado.cires.wod.iquodqc.check.api.GroupCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import java.util.Collection;
import java.util.List;

public class CompromiseGroupCheck extends GroupCastCheck {

  @Override
  public String getName() {
    return CheckNames.COMPROMISE_GROUP.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return List.of(
        CheckNames.ARGO_IMPOSSIBLE_DATE_TEST.getName(),
        CheckNames.ARGO_IMPOSSIBLE_LOCATION_TEST.getName(),
        CheckNames.EN_BACKGROUND_AVAILABLE_CHECK.getName(),
        CheckNames.ICDC_AQC_01_LEVEL_ORDER.getName(),
        CheckNames.CSIRO_SURFACE_SPIKES.getName(),
        CheckNames.IQUOD_GROSS_RANGE.getName(),
        CheckNames.WOD_RANGE_CHECK.getName(),
        CheckNames.AOML_CLIMATOLOGY.getName(),
        CheckNames.COTEDE_GTSPP_WOA_NORMBIAS.getName(),
        CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
        CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
        CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
        CheckNames.CSIRO_LONG_GRADIENT.getName(),
        CheckNames.ICDC_AQC_08_GRADIENT_CHECK.getName(),
        CheckNames.EN_STABILITY_CHECK.getName()
    );
  }
}
