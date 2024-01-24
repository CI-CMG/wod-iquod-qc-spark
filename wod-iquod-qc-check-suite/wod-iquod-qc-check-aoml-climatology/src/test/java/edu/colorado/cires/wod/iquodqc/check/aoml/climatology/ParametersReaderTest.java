package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.OptionalDouble;
import java.util.Properties;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import ucar.nc2.NetcdfFile;

public class ParametersReaderTest {

  private static final CoordinateReferenceSystem EPSG_4326;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:4326");
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
  }


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
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false, EPSG_4326);
      assertFalse(analyzedMean.isEmpty());
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
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false, EPSG_4326);
      assertFalse(analyzedMean.isEmpty());
      OptionalDouble analyzedSd = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", dataHolder, longitude, latitude, depth, true, EPSG_4326);
      assertFalse(analyzedSd.isEmpty());
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
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false, EPSG_4326);
      assertFalse(analyzedMean.isEmpty());
      OptionalDouble analyzedSd = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", dataHolder, longitude, latitude, depth, true, EPSG_4326);
      assertFalse(analyzedSd.isEmpty());
    } finally {
      ncFile.close();
    }
  }


  @Test
  public void testRead5() throws Exception {


    Properties properties = new Properties();
    properties.put("woa13_00_025.netcdf.uri", "ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
    WoaDataHolder dataHolder = ParametersReader.loadParameters(properties);
    double latitude = 49.329;
    double longitude = -124.0088;
    double depth = 251.58;

    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", dataHolder, longitude, latitude, depth, false, EPSG_4326);
      assertTrue(analyzedMean.isEmpty());
    } finally {
      ncFile.close();
    }
  }

}