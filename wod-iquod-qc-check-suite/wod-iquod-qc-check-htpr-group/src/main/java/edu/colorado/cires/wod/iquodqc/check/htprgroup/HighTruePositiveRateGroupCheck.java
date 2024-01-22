package edu.colorado.cires.wod.iquodqc.check.htprgroup;

import edu.colorado.cires.wod.iquodqc.check.api.GroupCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import java.util.Collection;
import java.util.List;

public class HighTruePositiveRateGroupCheck extends GroupCastCheck {

  @Override
  public String getName() {
    return CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return List.of(
        CheckNames.ARGO_IMPOSSIBLE_DATE_TEST.getName(),
        CheckNames.ARGO_IMPOSSIBLE_LOCATION_TEST.getName(),
        CheckNames.IQUOD_BOTTOM.getName(),
        CheckNames.EN_BACKGROUND_AVAILABLE_CHECK.getName(),
        CheckNames.ICDC_AQC_01_LEVEL_ORDER.getName(),
        CheckNames.CSIRO_SURFACE_SPIKES.getName(),
        CheckNames.CSIRO_WIRE_BREAK.getName(),
        CheckNames.IQUOD_GROSS_RANGE.getName(),
        CheckNames.ARGO_GLOBAL_RANGE_CHECK.getName(),
        CheckNames.EN_RANGE_CHECK.getName(),
//        CheckNames.ICDC_AQC_09_CLIMATOLOGY_CHECK.getName(),
        CheckNames.ICDC_AQC_10_CLIMATOLOGY_CHECK.getName(),
//        CheckNames.COTEDE_GTSPP_WOA_NORMBIAS.getName(),
        CheckNames.AOML_CLIMATOLOGY.getName(),
        CheckNames.EN_STD_LEV_BKG_AND_BUDDY_CHECK.getName(),
        CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
        CheckNames.CSIRO_CONSTANT_BOTTOM.getName(),
        CheckNames.AOML_CONSTANT.getName(),
        CheckNames.ICDC_AQC_06_N_TEMPERATURE_EXTREMA.getName(),
        CheckNames.ARGO_SPIKE_CHECK.getName(),
        CheckNames.COTEDE_TUKEY_53H_CHECK.getName(),
        CheckNames.ICDC_AQC_07_SPIKE_CHECK.getName(),
        CheckNames.AOML_SPIKE.getName(),
        CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName(),
        CheckNames.CSIRO_LONG_GRADIENT.getName(),
        CheckNames.AOML_GRADIENT.getName(),
        CheckNames.ICDC_AQC_08_GRADIENT_CHECK.getName(),
        CheckNames.CSIRO_SHORT_GRADIENT.getName()
//        CheckNames.COTEDE_ANOMALY_DETECTION_CHECK.getName()
    );
  }
}
