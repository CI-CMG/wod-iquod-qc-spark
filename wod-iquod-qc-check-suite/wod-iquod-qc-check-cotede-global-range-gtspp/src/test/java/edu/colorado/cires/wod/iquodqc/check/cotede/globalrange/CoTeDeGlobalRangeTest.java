package edu.colorado.cires.wod.iquodqc.check.cotede.globalrange;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class CoTeDeGlobalRangeTest {
  
  private static final double[] VALUES = {1,2,3,4,8,9,10,5,Double.NaN,7};
  
  private static final double MIN = 3;
  private static final double MAX = 8;
  
  private static final List<Integer> EXPECTED_FLAGS = List.of(0,1,5,6,8);
  
  @Test void testGetFlags() {
    assertEquals(EXPECTED_FLAGS, CoTeDeGlobalRange.checkGlobalRange(VALUES, MIN, MAX));
  }

}
