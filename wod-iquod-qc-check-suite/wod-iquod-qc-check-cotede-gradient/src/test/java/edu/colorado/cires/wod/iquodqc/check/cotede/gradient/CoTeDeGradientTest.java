package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeGradientTest {

  private static final double[] VALUES = {25.32, Double.NaN, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final List<Integer> FAILED_FLAGS = List.of(1, 10, 11, 14);
  private static final double THRESHOLD = 1.5;
  
  @Test void testGradientFlags() {
    assertEquals(FAILED_FLAGS, CoTeDeGradient.checkGradient(VALUES, THRESHOLD));
  }

}
