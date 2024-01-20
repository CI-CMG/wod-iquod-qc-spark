package edu.colorado.cires.wod.spark.iquodqc;

import static edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader.WOD_INFO_DGG4H6_PROP;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.wod.range.refdata.JsonParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CheckResolverTest {

  private Properties properties;

  public CheckResolverTest() {
    properties = new Properties();
    properties.put("EN_bgcheck_info.netcdf.uri", "https://www.metoffice.gov.uk/hadobs/en4/data/EN_bgcheck_info.nc");
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put("cars.netcdf.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/temperature_cars2009a.nc");
    properties.put("etopo5.netcdf.uri", "https://pae-paha.pacioos.hawaii.edu/thredds/ncss/etopo5?var=ROSE&disableLLSubset=on&disableProjSubset=on&horizStride=1&addLatLon=true");
    properties.put("climatological_t_median_and_amd_for_aqc.netcdf.uri", "https://s3-us-west-2.amazonaws.com/autoqc/climatological_t_median_and_amd_for_aqc.nc");
    properties.put("global_mean_median_quartiles_medcouple_smoothed.netcdf.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/global_mean_median_quartiles_medcouple_smoothed.nc");
    properties.put("wod_temp_min_max.netcdf.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/TEMP_MIN_MAX.nc");
    properties.put("wod_info_dgg4h6.mat.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/info_DGG4H6.mat");
    properties.put("wod_range_area.json.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/range_area.json");
    properties.put("wod_ranges_temperature.json.uri", "https://auto-qc-data.s3.us-west-2.amazonaws.com/WOD_ranges_Temperature.json");
    properties.put("data.dir", "../test-data");
  }

  @Test
  public void testAll() {

    // missing cars, constant cluster size

    List<CastCheck> checks = new ArrayList<>(CheckResolver.getChecks(Collections.emptySet(), properties));
    assertEquals(CheckNames.values().length, checks.size());
    Set<String> testsRan = new HashSet<>();
    Iterator<CastCheck> it = checks.iterator();
    while (it.hasNext()) {
      CastCheck check = it.next();
      it.remove();
      assertTrue(testsRan.containsAll(check.dependsOn()), "Out of order: " + check.getName() + " missing " + check.dependsOn());
      testsRan.add(check.getName());
    }
  }

  @Test
  public void testIquodFlags() {
    List<CastCheck> checks = new ArrayList<>(CheckResolver.getChecks(Collections.singleton(CheckNames.IQUOD_FLAGS_CHECK.getName()), properties));
    assertTrue(checks.size() < CheckNames.values().length);
    assertTrue(checks.size() > 4);
    List<String> names = checks.stream().map(CastCheck::getName).collect(Collectors.toList());
    assertTrue(names.contains(CheckNames.IQUOD_FLAGS_CHECK.getName()));
    assertTrue(names.contains(CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName()));
    assertTrue(names.contains(CheckNames.COMPROMISE_GROUP.getName()));
    assertTrue(names.contains(CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName()));
    assertFalse(names.contains(CheckNames.ICDC_AQC_04_MAX_OBS_DEPTH.getName()));
  }
}