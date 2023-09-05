package edu.colorado.cires.wod.iquodqc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Test;

class ObsUtilsTest {

  @Test
  public void t48tot68() throws Exception{
    double t68 = ObsUtils.t48tot68(100);
    assertEquals(t68,100D);

  }

  @Test
  public void t68tot90() throws Exception {
//    Check the temperature differences between ITPS-68 and -90 reported in
//    http://www.teos-10.org/pubs/gsw/pdf/t90_from_t48.pdf
    assertEquals(Math.round( 10000*(ObsUtils.t68tot90(10.0024) - 10)),0);
    assertEquals(Math.round( 10000*(ObsUtils.t68tot90(20.0048) - 20)),0);
    assertEquals(Math.round( 10000*(ObsUtils.t68tot90(30.0072) - 30)),0);
    assertEquals(Math.round( 10000*(ObsUtils.t68tot90(40.0096) - 40)),0);
  }

  @Test
  public void testDepthToPressure() throws Exception{
    double lat = 30;
    double depth = 10000;
    double truePressure = 10300.977189D;
    double pressure = ObsUtils.depthToPressure( depth, lat);
    assertTrue(Precision.equals(truePressure, pressure,0.01d));

  }

  @Test
  public void testPressureToDepth() throws Exception{
    assertTrue(Precision.equals(9713.735, ObsUtils.pressureToDepth(10000, 30.0),0.001d));
  }

  @Test
  public void testDensity() throws Exception {
    double[] t = {25., 20., 12.};
    double[] s = {35., 20., 40.};
    double[] p = {2000., 1000., 8000.};
    double[] rhoTrue = {1031.654229, 1017.726743, 1062.928258};
    Optional<Double> lat = Optional.empty();
    for (int i=0; i < t.length; i++) {
      assertTrue(Precision.equals(rhoTrue[i], ObsUtils.density(t[i], s[i], p[i], lat),0.0001d));
    }
  }
}