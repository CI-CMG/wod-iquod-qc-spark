package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import static org.junit.jupiter.api.Assertions.*;

import java.util.OptionalDouble;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import ucar.nc2.NetcdfFile;

public class ParametersReaderTest {


  @Test
  public void testRead() throws Exception {
    Properties properties = new Properties();
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
    WoaDataHolder dataHolder = ParametersReader.loadParameters(properties);
    double longitude = 0d;
    double latitude = 0d;
    double depth = 100d;
    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false);
    } finally {
      ncFile.close();
    }
  }

  @Test
  public void testRead2() throws Exception {
    Properties properties = new Properties();
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
    WoaDataHolder dataHolder = ParametersReader.loadParameters(properties);
    double latitude = 88.7088;
    double longitude = -118.907;
    double depth = 100d;

    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false);
      OptionalDouble analyzedSd = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", dataHolder, longitude, latitude, depth, true);
    } finally {
      ncFile.close();
    }
  }

  @Test
  public void testRead3() throws Exception {
    Properties properties = new Properties();
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
    WoaDataHolder dataHolder = ParametersReader.loadParameters(properties);
    double latitude = 83.587;
    double longitude = 87.2555;
    double depth = 100d;

    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false);
      OptionalDouble analyzedSd = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", dataHolder, longitude, latitude, depth, true);
    } finally {
      ncFile.close();
    }
  }

  @Test
  public void testRead4() throws Exception {
    Properties properties = new Properties();
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
    WoaDataHolder dataHolder = ParametersReader.loadParameters(properties);
    double latitude = 88.6938;
    double longitude = 60.1785;
    double depth = -10000d;

    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false);
      OptionalDouble analyzedSd = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", dataHolder, longitude, latitude, depth, true);
    } finally {
      ncFile.close();
    }
  }

}