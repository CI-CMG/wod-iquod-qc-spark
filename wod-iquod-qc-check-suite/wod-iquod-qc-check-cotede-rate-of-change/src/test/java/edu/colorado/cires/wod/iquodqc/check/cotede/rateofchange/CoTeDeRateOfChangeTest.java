package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeRateOfChangeTest {
  private static final double[] VALUES = {25.32, 25.34, 25.34, Double.NaN, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final List<Integer> EXPECTED_FLAGS = List.of(3, 9, 10, 14);
  private static final double THRESHOLD = 4;
  
  @Test void testRateOfChangeFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeRateOfChange.checkRateOfChange(VALUES, THRESHOLD));
  }

}
