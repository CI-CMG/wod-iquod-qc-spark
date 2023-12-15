package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import static edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea.WodLooseLocationAtSea.checkLooseLocationAtSea;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class WodLooseLocationAtSeaTest {
  
  private static final int BUFFER_WIDTH = 2;
  private static final String ETOPO_LOCATION = "src/main/resources/data/etopo5.nc";
  
  @ParameterizedTest
  @CsvSource({
      "-91,10",
      "91,0",
      "0,-181",
      "0,361"
  })
  public void testInvalidLocations(double lat, double lon) {
    assertFalse(checkLooseLocationAtSea(lat, lon, BUFFER_WIDTH, mock(NetcdfFile.class)));
  }
  
  @Test void testOceanPoint() {
    assertTrue(runCheckLooseLocationAtSea(4.10566666667, -38.0133333333));
  }
  
  @Test void testLandPoint() {
    assertFalse(runCheckLooseLocationAtSea(-4.10566666667, -39));
  }
  
  @Test void testCoastPoint() {
    assertTrue(runCheckLooseLocationAtSea(-4.1, -38.15));
  }
  
  @Test void test180Lon() {
    assertTrue(runCheckLooseLocationAtSea(-4.1, -180));
  }
  
  @Test void test90Lat180Lon() {
    assertTrue(runCheckLooseLocationAtSea(90, 180));
  }
  
  private static boolean runCheckLooseLocationAtSea(double lat, double lon) {
    try (NetcdfFile file = NetcdfFiles.open(ETOPO_LOCATION)) {
      return checkLooseLocationAtSea(lat, lon, BUFFER_WIDTH, file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
