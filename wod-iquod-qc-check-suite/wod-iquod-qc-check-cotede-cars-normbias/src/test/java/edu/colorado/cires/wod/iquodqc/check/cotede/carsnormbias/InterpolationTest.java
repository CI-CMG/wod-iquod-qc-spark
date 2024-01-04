package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import static edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.InterpolationUtils.interpolate3D;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class InterpolationTest {
  
  private static final double[] DATA = new double[] { 28.752491111348483, 28.714800177010055, 28.755970274518184,
      28.730456411273707, 28.744373063952512, 28.701463384859537, 28.769886927196985, 28.742633482367662 };
  private static final double[] LAT_VALS = { 4, 4.5 };
  private static final double[] LON_VALS = { 180, 180.5 };
  private static final double[] DEPTH_VALS = { 0, 5 };
  
  @Test void testInterpolate() {
    double lat = 4.1;
    double lon = 180.1;
    double depth = 1;
    
    assertEquals(
        28.74564875711474,
        interpolate3D(
            new double[][][] {
                {
                    { DATA[0], DATA[2] },
                    { DATA[4], DATA[6] }
                },
                {
                    { DATA[1], DATA[3] },
                    { DATA[5], DATA[7] }
                }
            },
            LAT_VALS,
            LON_VALS,
            DEPTH_VALS,
            lat,
            lon,
            depth
        ),
        1e-3
    );
  }

}
