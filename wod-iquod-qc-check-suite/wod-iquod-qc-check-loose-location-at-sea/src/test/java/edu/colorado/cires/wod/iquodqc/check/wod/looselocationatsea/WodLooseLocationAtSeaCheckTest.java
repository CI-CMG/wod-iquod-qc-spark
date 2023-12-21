package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.EtopoParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WodLooseLocationAtSeaCheckTest {
  
  private static WodLooseLocationAtSeaCheck check = new WodLooseLocationAtSeaCheck();
  
  @BeforeAll
  public static void beforeAll() {
    Properties properties = new Properties();
    properties.put("etopo5.netcdf.uri",
        "https://pae-paha.pacioos.hawaii.edu/thredds/ncss/etopo5?var=ROSE&disableLLSubset=on&disableProjSubset=on&horizStride=1&addLatLon=true");
    properties.put("data.dir", "../../test-data");
    
    CastCheckInitializationContext context = mock(CastCheckInitializationContext.class);
    when(context.getProperties()).thenReturn(properties);
    check.initialize(context);
    EtopoParametersReader.loadParameters(properties);
  }
  
  @Test void testPass() {
    Cast cast = Cast.builder()
        .withLatitude(-4.1)
        .withLongitude(-38.15)
        .withDepths(Collections.singletonList(
            Depth.builder()
                .withDepth(100D)
                .build()
        ))
        .build();

    assertEquals(0, check.getFailedDepths(cast).size());
  }

  @Test void testFail() {
    Cast cast = Cast.builder()
        .withLatitude(-4.10566666667)
        .withLongitude(-39)
        .withDepths(List.of(
            Depth.builder()
                .withDepth(100D)
                .build(),
            Depth.builder()
                .withDepth(200D)
                .build()
        ))
        .build();

    Collection<Integer> failures = check.getFailedDepths(cast);
    assertEquals(List.of(0, 1), failures);
  }

}
