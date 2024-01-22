package edu.colorado.cires.wod.iquodqc.common;

public enum CheckNames {
  AOML_CLIMATOLOGY("AOML_climatology_test"),
  AOML_CONSTANT("AOML_constant"),
  AOML_GRADIENT("AOML_gradient"),
  AOML_GROSS("AOML_gross"),
  AOML_SPIKE("AOML_spike"),
  ARGO_GLOBAL_RANGE_CHECK("Argo_global_range_check"),
  ARGO_GRADIENT_TEST("Argo_gradient_test"),
  ARGO_IMPOSSIBLE_DATE_TEST("Argo_impossible_date_test"),
  ARGO_IMPOSSIBLE_LOCATION_TEST("Argo_impossible_location_test"),
  ARGO_PRESSURE_INCREASING_TEST("Argo_pressure_increasing_test"),
  ARGO_REGIONAL_RANGE_TEST("Argo_regional_range_test"),
  COTEDE_ARGO_DENSITY_INVERSION_CHECK("CoTeDe_Argo_density_inversion"),
  COTEDE_LOCATION_AT_SEA_TEST("CoTeDe_location_at_sea_test"),
  COTEDE_RATE_OF_CHANGE("CoTeDe_rate_of_change"),
  COTEDE_SPIKE_CHECK("CoTeDe_spike"),
  COTEDE_SPIKE_GTSPP_CHECK("CoTeDe_GTSPP_spike_check"),
  COTEDE_GTSPP_WOA_NORMBIAS("CoTeDe_GTSPP_WOA_normbias"),
  COTEDE_WOA_NORMBIAS("CoTeDe_WOA_normbias"),
  CSIRO_CONSTANT_BOTTOM("CSIRO_constant_bottom"),
  CSIRO_DEPTH("CSIRO_depth"),
  CSIRO_LONG_GRADIENT("CSIRO_long_gradient"),
  CSIRO_SHORT_GRADIENT("CSIRO_short_gradient"),
  CSIRO_SURFACE_SPIKES("CSIRO_surface_spikes"),
  CSIRO_WIRE_BREAK("CSIRO_wire_break"),
  EN_BACKGROUND_CHECK("EN_background_check"),
  EN_BACKGROUND_AVAILABLE_CHECK("EN_background_available_check"),
  EN_CONSTANT_VALUE_CHECK("EN_constant_value_check"),
  EN_INCREASING_DEPTH_CHECK("EN_increasing_depth_check"),
  EN_RANGE_CHECK("EN_range_check"),
  EN_SPIKE_AND_STEP_CHECK("EN_spike_and_step_check"),
  EN_SPIKE_AND_STEP_SUSPECT("EN_spike_and_step_suspect"),
  EN_STABILITY_CHECK("EN_stability_check"),
  EN_STD_LEV_BKG_AND_BUDDY_CHECK("EN_std_lev_bkg_and_buddy_check"),
  ICDC_AQC_01_LEVEL_ORDER("ICDC_aqc_01_level_order"),
  IQUOD_BOTTOM("IQUOD_bottom"),
  HIGH_TRUE_POSITIVE_RATE_GROUP("HTPR_check"),
  LOW_TRUE_POSITIVE_RATE_GROUP("LTPR_check"),
  COMPROMISE_GROUP("COMP_check"),
  IQUOD_GROSS_RANGE("IQuOD_gross_range_check"),
  ICDC_AQC_09_CLIMATOLOGY_CHECK("ICDC_aqc_09_climatology_check"),
  ICDC_AQC_10_CLIMATOLOGY_CHECK("ICDC_aqc_10_climatology_check"),
  ARGO_SPIKE_CHECK("Argo_spike_test"),
  ICDC_AQC_06_N_TEMPERATURE_EXTREMA("ICDC_aqc_06_n_temperature_extrema"),
  COTEDE_TUKEY_53H_CHECK("CoTeDe_tukey_53H_check"),
  COTEDE_TUKEY_53_NORM_CHECK("CoTeDe_tukey53H_norm"),
  ICDC_AQC_07_SPIKE_CHECK("ICDC_aqc_07_spike_check"),
  ICDC_AQC_08_GRADIENT_CHECK("ICDC_aqc_08_gradient_check"),
  COTEDE_ANOMALY_DETECTION_CHECK("CoTeDe_anomaly_detection_check"),
  COTEDE_DIGIT_ROLLOVER("CoTeDe_digit_roll_over"),
  COTEDE_GLOBAL_RANGE_GTSPP_CHECK("CoTeDe_GTSPP_global_range"),
  COTEDE_GRADIENT_CHECK("CoTeDe_gradient"),
  COTEDE_GRADIENT_GTSPP_CHECK("CoTeDe_GTSPP_gradient"),
  WOD_RANGE_CHECK("wod_range_check"),
  WOD_LOOSE_LOCATION_AT_SEA_CHECK("wod_loose_location_at_sea_check"),
  MIN_MAX_CHECK("minmax"),
  GTSPP_PROFILE_ENVELOP_CHECK("CoTeDe_GTSPP_profile_envelop"),
  WOD_GRADIENT_CHECK("WOD_gradient_check"),
  ICDC_AQC_02_CRUDE_RANGE("ICDC_aqc_02_crude_range"),
  ICDC_AQC_04_MAX_OBS_DEPTH("ICDC_aqc_04_max_obs_depth"),
  ICDC_AQC_05_STUCK_VALUE("ICDC_aqc_05_stuck_value"),
  IQUOD_FLAGS_CHECK("IQuOD_flags_check"),
  COTEDE_CONSTANT_CLUSTER_SIZE_CHECK("CoTeDe_Constant_Cluster_Size_check"),
  COTEDE_CARS_NORMBIAS_CHECK("CoTeDe_Cars_Normbias_check");

  private final String name;

  CheckNames(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
