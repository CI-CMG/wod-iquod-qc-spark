package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import static edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.CoTeDeCarsNormbias.computeNormbias;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ucar.ma2.InvalidRangeException;

public class CoTeDeCarsNormbiasTest {

  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58,
      Double.NaN};
  private static final double[] DEPTHS = {2, 6, 10, 21, 44, 79, 100, 150, 200, 400, 410, 650, 1000, 2000, 5000};
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

  private static CarsParameters carsParameters;

  @BeforeAll
  static void beforeAll() {
    Properties properties = new Properties();
    properties.put(CarsParametersReader.CARS_NC_PROP, "https://auto-qc-data.s3.us-west-2.amazonaws.com/temperature_cars2009a.nc");
    properties.put("data.dir", "../../test-data");

    carsParameters = CarsParametersReader.loadParameters(properties);
  }

  @ParameterizedTest
  @CsvSource({
      "15,-38",
      "15.00000001,-38.00000001",
      "15.00000001,-38",
      "15,-38.00000001",
  })
  public void testComputeCarsNormbias(double latitude, double longitude) throws InvalidRangeException, IOException {
    assertArrayEquals(
        EXPECTED_NORMBIAS,
        computeNormbias(
            latitude, longitude, TEMPERATURES, DEPTHS, carsParameters
        ).stream().mapToDouble(v -> v).toArray(),
        1e-7
    );
  }
}
