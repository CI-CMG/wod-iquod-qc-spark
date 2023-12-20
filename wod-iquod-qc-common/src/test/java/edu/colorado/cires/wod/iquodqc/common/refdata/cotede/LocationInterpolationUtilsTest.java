package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocationInterpolationUtilsTest {

  private static final Path DIR = Paths.get("target/nctest");
  private NetCdfEtopoParameters parameters;

  @BeforeEach
  public void before() throws Exception {
    Files.createDirectories(DIR);
    Properties properties = new Properties();
    properties.put(FileDownloader.DATA_DIR_PROP, DIR.toString());
    properties.put(EtopoParametersReader.ETOPO5_NC_PROP,
        "https://pae-paha.pacioos.hawaii.edu/thredds/ncss/etopo5?var=ROSE&disableLLSubset=on&disableProjSubset=on&horizStride=1&addLatLon=true");
    parameters = EtopoParametersReader.loadParameters(properties);
  }

  @Test
  public void test() {
    assertEquals(-366D, LocationInterpolationUtils.depthInterpolation(parameters, 30D, 10D), 0.0001);
    assertEquals(5192.5, LocationInterpolationUtils.depthInterpolation(parameters, -30D, 10D), 0.0001);
    assertEquals(5019.0825, LocationInterpolationUtils.depthInterpolation(parameters, -38D, 15D), 0.0001);
    assertEquals(4995.0496, LocationInterpolationUtils.depthInterpolation(parameters, -138D, 12D), 0.0001);
    assertEquals(4876D, LocationInterpolationUtils.depthInterpolation(parameters, 0D, 0D), 0.0001);
    assertEquals(-76D, LocationInterpolationUtils.depthInterpolation(parameters, 0D, 6D), 0.0001);
    assertEquals(5454D, LocationInterpolationUtils.depthInterpolation(parameters, 0D, -10D), 0.0001);
  }
}