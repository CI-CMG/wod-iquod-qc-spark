package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeSpikeTest {
  
  private static final double[] VALUES = {25.32, 25.34, Double.NaN, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final int THRESHOLD = 4;

  @Test void testSpikeCheck() {
    assertEquals(List.of(2, 9, 14), CoTeDeSpike.checkSpike(VALUES, THRESHOLD));
  }

}
