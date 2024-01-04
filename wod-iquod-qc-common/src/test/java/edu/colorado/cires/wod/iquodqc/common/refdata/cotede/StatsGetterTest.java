package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StatsGetterTest {

  private static final Path DIR = Paths.get("target/nctest");
  private WoaParameters parameters;

  @BeforeEach
  public void before() throws Exception {
    Files.createDirectories(DIR);
    Properties properties = new Properties();
    properties.put(FileDownloader.DATA_DIR_PROP, DIR.toString());
    properties.put(WoaParametersReader.WOA_S1_NC_PROP,
        "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put(WoaParametersReader.WOA_S2_NC_PROP,
        "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put(WoaParametersReader.WOA_S3_NC_PROP,
        "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put(WoaParametersReader.WOA_S4_NC_PROP,
        "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    parameters = WoaParametersReader.loadParameters(properties);
  }


  @ParameterizedTest
  @CsvSource({
      "5,16,0,17.5,-37.5,24.64101600",
      "8,16,0,17.5,-37.5,26.39279174",
      "5,16,10,17.5,-37.5,24.6360282",
      "5,16,0,12.5,-37.5,25.1675281",
      "5,16,0,17.5,-32.5,23.9796047",
      "5,16,10,12.5,-37.5,25.1904888",
      "5,16,0,-14.03,62.5,27.5327732",
      "5,16,0,-12.5,62.03,27.7773725",
      "5,16,0,17.5,-177.5,26.46188926",
      "5,16,10,17.5,-32.5,24.062252044",
      "3,31,0,17.5,-37.5,24.0034523",
      "1,10,0,10,-30,25.3084492",
      "1,10,10,10,-30,25.35460878",
      "2,3,0,10,-30,25.308449268",
      "4,30,0,10,-30,26.0169248580",
      "4,30,300,10,-30,10.39782374",
      "4,30,0,12,-25,25.03071006"
  })
  public void test(int month, int day, double depth, double lat, double lon, double expected) throws Exception {
    WoaGetter woaGetter = new WoaGetter(parameters);
    LocalDate date = LocalDate.of(2023, month, day);
    assertEquals(expected, woaGetter.getStats(date.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli(), depth, lon, lat).getMean().getAsDouble(),
        0.000001);
  }


  @ParameterizedTest
  @CsvSource({
      "6,4,2,48.1953,-69.5855",
      "6,4,5,48.1953,-69.5855",
      "6,4,6,48.1953,-69.5855",
      "6,4,21,48.1953,-69.5855",
      "6,4,44,48.1953,-69.5855",
      "6,4,79,48.1953,-69.5855",
      "6,4,5000,48.1953,-69.5855",
      "5,16,5502,17.5,-37.5",
      "4,18,0,4,-38",
      "4,18,10,4,-38",
      "4,18,100,4,-38",
      "4,18,300,4,-38",
      "4,18,1000,4,-38",
      "4,18,2000,4,-38",
      "4,18,4000,4,-38",
      "4,18,4700,4,-38",
      "4,18,5000,4,-38",
      "3,31,0,-19.9,-43.9",
  })
  public void testInvalidMissing(int month, int day, double depth, double lat, double lon) throws Exception {
    WoaGetter woaGetter = new WoaGetter(parameters);
    LocalDate date = LocalDate.of(2023, month, day);
    assertTrue(woaGetter.getStats(date.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli(), depth, lon, lat).getMean().isEmpty());
  }

  @ParameterizedTest
  @CsvSource({
      "1,10,0,10,-30,25.30844926,0.02763340,0.8664511,1102",
      "1,10,10,10,-30,25.35460878,0.0270526,0.8527680,1078",
  })
  public void test(int month, int day, double depth, double lat, double lon, double mean, double error, double sd, int obs) throws Exception {
    WoaGetter woaGetter = new WoaGetter(parameters);
    LocalDate date = LocalDate.of(2023, month, day);
    WoaStats stats = woaGetter.getStats(date.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli(), depth, lon, lat);
    assertEquals(mean, stats.getMean().getAsDouble(), 0.000001);
    assertEquals(error, stats.getStandardError().getAsDouble(), 0.000001);
    assertEquals(sd, stats.getStandardDeviation().getAsDouble(), 0.000001);
    assertEquals(obs, stats.getNumberOfObservations().getAsInt());
  }
}