package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import static edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.CoTeDeCarsNormbias.computeCarsNormbiases;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class CoTeDeCarsNormbiasTest {
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] DEPTHS = {   2,    6,   10,   21,   44,   79,  100,  150,  200, 400,  410,  650, 1000, 2000, 5000};
  private static final long TIMESTAMP = LocalDateTime.of(2016, 6, 4, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  private static final String FILE_PATH = "/Users/paytoncain/Desktop/temperature_cars2009a.nc";
  private static final String MEAN_VARIABLE_NAME = "mean";
  private static final String STANDARD_DEVIATION_VARIABLE_NAME = "std_dev";
  private static final String SCALE_FACTOR_ATTRIBUTE_NAME = "scale_factor";
  private static final String ADD_OFFSET_ATTRIBUTE_NAME = "add_offset";
  private static final String DEPTH_VARIABLE_NAME = "depth";
  private static final String LAT_VARIABLE_NAME = "lat";
  private static final String LON_VARIABLE_NAME = "lon";
  private static final double[] EXPECTED_NORMBIAS = {
      -0.38019257069748696,
      -0.393885671921776,
      -0.37565610075965755,
      -0.3799254381742379,
      -0.4762608025493148,
      0.06060039754158271,
      0.07226629854667979,
      0.08483219688898401,
      0.12914847336393787,
      1.3081374269005843,
      -8.640890659107448,
      0.9686821986158043,
      -0.4231785925487893,
      0.931676201372987, 
      Double.NaN
  };
  
  private static CarsGetter carsGetter;
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put(CarsParametersReader.CARS_NC_PROP, "https://www.marine.csiro.au/atlas/export/temperature_cars2009a.nc.gz");
    properties.put("data.dir", "../../test-data");

    CarsParameters carsParameters = CarsParametersReader.loadParameters(properties);
    carsGetter = new CarsGetter(carsParameters);
  }
  
  @ParameterizedTest
  @CsvSource({
      "15,-38",
      "15.00000001,-38.00000001",
      "15.00000001,-38",
      "15,-38.00000001",
  })
  public void testComputeCarsNormbias(double latitude, double longitude) {
    assertArrayEquals(
        EXPECTED_NORMBIAS,
        computeCarsNormbiases(
            TEMPERATURES, DEPTHS, latitude, longitude, carsGetter
        ), 
        1e-1
    );
  }
}
