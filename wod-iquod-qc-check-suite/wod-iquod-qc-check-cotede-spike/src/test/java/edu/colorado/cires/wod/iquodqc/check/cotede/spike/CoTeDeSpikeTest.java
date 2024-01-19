package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeSpikeTest {

  private static final double[] VALUES = {25.32, 25.34, Double.NaN, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final int THRESHOLD = 4;

  private static final double[] EXPECTED_SPIKES = {Double.NaN,  1.77635684e-15,  0.00000000e+00, -3.00000000e-02, -3.20000000e-01, -1.53000000e+00, -1.61000000e+00, -2.56000000e+00, -2.56000000e+00, -4.15000000e+00,  1.00000000e+00,  1.00000000e+00, -2.13000000e+00, Double.NaN, Double.NaN};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  @Test void testSpikeCheck() {
    assertEquals(List.of(2, 9, 14), CoTeDeSpike.getFlags(
        VALUES,
        CoTeDeSpike.computeSpikes(VALUES),
        THRESHOLD
    ));
  }

  @Test void testComputeSpikes() {
    assertArrayEquals(EXPECTED_SPIKES, CoTeDeSpike.computeSpikes(TEMPERATURES), 1e-14);
  }

}
