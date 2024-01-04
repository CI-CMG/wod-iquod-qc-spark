package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CoTeDeWoaNormbiasTest {
  
  private static final double[] EXPECTED_WOA_NORMBIAS = {0.01958676, 0.04645438, 0.03666594, 0.05105439, 0.11075098, 0.18418772, 0.13581132, 0.04307891, -0.01186448, -0.00556727, -5.22259791, 0.01277138, -0.23932256, -0.06257082, Double.NaN};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] DEPTHS = {2, 6, 10, 21, 44, 79, 100, 150, 200,
      400, 410, 650, 1000, 2000, 5000};
  
  private static WoaGetter woaGetter;
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put("data.dir", "../../test-data");
    
    woaGetter = new WoaGetter(WoaParametersReader.loadParameters(properties));
  }
  
  @Test void testComputeNormbias() {
    assertArrayEquals(
        EXPECTED_WOA_NORMBIAS,
        CoTeDeWoaNormbias.computeNormBiases(
            LocalDateTime.of(2016, 6, 4, 0, 0)
                .atZone(ZoneId.of("UTC"))
                .toInstant()
                .toEpochMilli(),
            -38,
            15,
            DEPTHS,
            TEMPERATURES,
            woaGetter
        ),
        1e+1
    );
  }

}
