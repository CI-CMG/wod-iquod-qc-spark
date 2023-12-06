package edu.colorado.cires.wod.iquodqc.check.cotede.digitrollover;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeDigitRolloverTest {

  private static final List<Integer> EXPECTED_FAILED_INDICES = List.of(7, 8, 9, 10, 14);
  private static final List<Double> VALUES = List.of(25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN);
  private static final double THRESHOLD = 2.5;

  @Test
  void testDigitRollover() {
    for (int i = 1; i < VALUES.size(); i++) {
      assertEquals(!EXPECTED_FAILED_INDICES.contains(i), CoTeDeDigitRollover.checkDigitRollover(VALUES.get(i), VALUES.get(i - 1), THRESHOLD));
    }
  }

}
