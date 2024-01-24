package edu.colorado.cires.wod.iquodqc.check.icdcaqc09.climatology;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.icdcaqc09.climatology.refdata.ClimatologicalTMedianAndAmdForAqcParametersReader;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ICDCAqc09ClimatologyCheckTest {

  private static final Path DATA_PATH = Paths.get("src/test/resources/test-files/data.txt");

  private static final ICDCAqc09ClimatologyCheck CHECK = new ICDCAqc09ClimatologyCheck();

  @BeforeAll
  static void beforeAll() {
    Properties properties = new Properties();
    properties.put(ClimatologicalTMedianAndAmdForAqcParametersReader.CLIMATOLOGICAL_T_MEDIAN_AND_AMD_FOR_AQC_NC_PROP,
        "https://s3-us-west-2.amazonaws.com/autoqc/climatological_t_median_and_amd_for_aqc.nc");
    properties.put("data.dir", "../../test-data");

    CastCheckInitializationContext context = mock(CastCheckInitializationContext.class);
    when(context.getProperties()).thenReturn(properties);

    CHECK.initialize(context);
    ICDCAqc09ClimatologyCheck.load(properties);
  }

  private List<ICDCdata> getTestData() {
    List<ICDCdata> icdCdata = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(DATA_PATH)) {
      String line = reader.readLine();
      while (line != null) {
        if (line.startsWith("HH")) {
          ICDCdata d = new ICDCdata(line);
          for (int i = 0; i < d.getRows(); i++) {
            line = reader.readLine();
            d.addDepth(i, line);
          }
          icdCdata.add(d);
        }
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return icdCdata;
  }

  private Cast buildCast(ICDCdata data) {
    return Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLatitude(data.getLatitude())
        .withLongitude(data.getLongitude())
        .withYear(data.getYear())
        .withMonth(data.getMonth())
        .withDay(data.getDay())
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.emptyList())
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(data.getCastnumber())
        .withCountry(data.getCountry())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build(),
            Attribute.builder()
                .withCode(PROBE_TYPE)
                .withValue(data.getProbeType())
                .build()
        ))
        .withDepths(data.getDepths())
        .build();
  }

  @Test
  void testCheck() {
    List<ICDCdata> testData = getTestData();

    testData.forEach(d -> {
      Cast cast = buildCast(d);
      Collection<Integer> failed = CHECK.getFailedDepths(cast);
      assertEquals(d.getFailures(), failed);
    });
  }

}
