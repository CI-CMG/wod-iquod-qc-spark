package edu.colorado.cires.wod.iquodqc.check.en.stability;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class EnStabilityCheckFunctionsTest {

  @Test
  public void testMcdougallEOS() {
    assertEquals(1031.654229, EnStabilityCheckFunctions.mcdougallEOS(35D, 25D, 2000D), 0.000001);
    assertEquals(1017.726743, EnStabilityCheckFunctions.mcdougallEOS(20D, 20D, 1000D), 0.000001);
    assertEquals(1062.928258, EnStabilityCheckFunctions.mcdougallEOS(40D, 12D, 8000D), 0.000001);
  }

  @Test
  public void testMcdougallPotentialTemperature() {
    assertEquals(19.621967, EnStabilityCheckFunctions.potentialTemperature(35D, 20D, 2000D), 0.000001);
  }
}