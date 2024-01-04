package edu.colorado.cires.wod.iquodqc.check.icdcaqc10.climatology;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.icdcaqc10.climatology.refdata.GlobalMedianQuartilesMedcoupleSmoothedParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ICDCAqc10ClimatologyCheckTest {
  
  private static final ICDCAqc10ClimatologyCheck CHECK = new ICDCAqc10ClimatologyCheck();
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put(GlobalMedianQuartilesMedcoupleSmoothedParametersReader.GLOBAL_MEAN_MEDIAN_QUARTILES_MEDCOUPLE_SMOOTHED_NC_PROP, "https://auto-qc-data.s3.us-west-2.amazonaws.com/global_mean_median_quartiles_medcouple_smoothed.nc");
    properties.put("data.dir", "../../test-data");
    
    CastCheckInitializationContext context = mock(CastCheckInitializationContext.class);
    when(context.getProperties()).thenReturn(properties);

    CHECK.initialize(context);
    ICDCAqc10ClimatologyCheck.load(properties);
  }

  @ParameterizedTest
  @CsvSource({
      "0,5,false",
      "5,5,true",
      "12,5,false"
  })
  public void testRanges(double temperature, double depth, boolean shouldPass) {
    Cast cast = Cast.builder()
        .withLatitude(50.0)
        .withLongitude(-180.0)
        .withTimestamp(
            LocalDateTime.of(2000, 1, 15, 0, 0)
                .atZone(
                    ZoneId.of("UTC")
                )
                .toInstant()
                .toEpochMilli()
        )
        .withDepths(
            Collections.singletonList(
                Depth.builder()
                    .withDepth(depth)
                    .withData(
                        Collections.singletonList(
                            ProfileData.builder()
                                .withValue(temperature)
                                .withVariableCode(TEMPERATURE)
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build();

    Collection<Integer> failed = CHECK.getFailedDepths(cast);
    assertEquals(shouldPass ? Collections.emptyList() : Collections.singletonList(0), failed);
  }
  
  @ParameterizedTest
  @CsvSource({
      "-80,0,true",
      "-70,0,false",
      "-70,-182,true",
      "-70,362,true",
      "-70,182,false",
  })
  public void testLocations(double latitude, double longitude, boolean shouldPass) {
    Cast cast = Cast.builder()
        .withLatitude(latitude)
        .withLongitude(longitude)
        .withTimestamp(
            LocalDateTime.of(2000, 1, 15, 0, 0)
                .atZone(
                    ZoneId.of("UTC")
                )
                .toInstant()
                .toEpochMilli()
        )
        .withDepths(
            List.of(
                Depth.builder()
                    .withDepth(1)
                    .withData(
                        Collections.singletonList(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(-5)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withDepth(2)
                    .withData(
                        Collections.singletonList(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(-5)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withDepth(5)
                    .withData(
                        Collections.singletonList(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(-5)
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build();

    Collection<Integer> failed = CHECK.getFailedDepths(cast);
    assertEquals(
        shouldPass ? Collections.emptyList() : IntStream.range(0, 3).boxed().collect(Collectors.toList()), 
        failed
    );
  }
  
}
