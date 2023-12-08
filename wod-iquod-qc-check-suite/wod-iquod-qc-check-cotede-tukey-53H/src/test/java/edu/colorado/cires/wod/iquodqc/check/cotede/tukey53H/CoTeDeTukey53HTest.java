package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;


public class CoTeDeTukey53HTest {
  private static final double[] VALUES = {Double.NaN, 25.34, 25.34, 25.31, 24.99, 230.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  private static final List<Integer> EXPECTED_FLAGS = List.of(0, 5, 14);
  
  private static final double THRESHOLD = 6D;
  
  @Test void testTukeyFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeTukey53H.checkTukey53H(VALUES, THRESHOLD));
  }
}
