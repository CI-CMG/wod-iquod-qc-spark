package edu.colorado.cires.wod.iquodqc.check.wod.range;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.wod.range.refdata.JsonParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WodRangeCheckTest {
  
  private static WodRangeCheck check = new WodRangeCheck();
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put(JsonParametersReader.WOD_RANGE_AREA_PROP,
        "https://auto-qc-data.s3.us-west-2.amazonaws.com/range_area.json");
    properties.put(JsonParametersReader.WOD_RANGES_TEMPERATURE_PROP,
        "https://auto-qc-data.s3.us-west-2.amazonaws.com/WOD_ranges_Temperature.json");
    properties.put("data.dir", "../../test-data");

    CastCheckInitializationContext context = mock(CastCheckInitializationContext.class);
    when(context.getProperties()).thenReturn(properties);
    
    check.initialize(context);
  }
  
  @Test void testTemperaturePass() {
    Cast cast = Cast.builder()
        .withLatitude(-89.5)
        .withLongitude(0.5)
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(15.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(50.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(-2.4)
                                .build()
                        )
                    ).build()
            )
        ).build();
    
    assertEquals(0, new WodRangeCheck().getFailedDepths(cast).size());
  }
  
  @Test void testTemperatureFailure() {
    Cast cast = Cast.builder()
        .withLatitude(-89.5)
        .withLongitude(0.5)
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(0.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(2400)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(10.)
                                .build()
                        )
                    ).build()
            )
        ).build();

    Collection<Integer> failedCasts = new WodRangeCheck().getFailedDepths(cast);
    assertEquals(1, failedCasts.size());
    assertEquals(List.of(1), failedCasts);
  }

}
