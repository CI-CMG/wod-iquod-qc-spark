package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class WodLooseLocationAtSeaCheckTest {
  
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

    assertEquals(0, new WodLooseLocationAtSeaCheck().getFailedDepths(cast).size());
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

    Collection<Integer> failures = new WodLooseLocationAtSeaCheck().getFailedDepths(cast);
    assertEquals(List.of(0, 1), failures);
  }

}
