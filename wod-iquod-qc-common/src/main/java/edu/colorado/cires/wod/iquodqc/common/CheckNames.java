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
  ARGO_REGIONAL_RANGE_TEST("Argo_regional_range_test"),
  COTEDE_LOCATION_AT_SEA_TEST("CoTeDe_location_at_sea_test"),
  COTEDE_GTSPP_WOA_NORMBIAS("CoTeDe_GTSPP_WOA_normbias"),
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
  IQUOD_BOTTOM("IQUOD_bottom");

  private final String name;

  CheckNames(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
