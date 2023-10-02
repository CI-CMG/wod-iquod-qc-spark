package edu.colorado.cires.wod.iquodqc.check.icdcaqc02.cruderange;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class IcdcAqc02CrudeRangeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "ICDC_aqc_02_crude_range";
  }
/*
     def test(p, parameters, data_store):
    '''Return a set of QC decisions.
    '''

    nlevels, z, t = ICDC.reordered_data(p, data_store)

    qc = (t < parminover) | (t > parmaxover)

    for i, tval in enumerate(t):
        if qc[i]: continue # Already rejected.

        zval = z[i]

        if np.any((tval >= tcrude1) & (tval <= tcrude2) &
                  (zval <= zcrude1) & (zval >= zcrude2)):
            qc[i] = True

    return ICDC.revert_qc_order(p, qc, data_store)

# Ranges:
tcrude1 = np.array(
     [-3.5,-2.5,-2.3,-2.0,-1.4,-1.0,-0.8,-0.5,-0.4,3.0,
      5.0,5.5,6.0,7.0,7.5,8.8,9.5,10.0,11.0,12.0,12.8,13.5,
      14.75,15.0,16.0,17.5,18.5,19.0,20.0,21.4,22.4,23.0,24.0,
      26.0,28.0,31.0,32.0])

tcrude2 = np.array(
     [-2.5,-2.3,-2.0,-1.4,-1.0,-0.8,-0.5,-0.4,3.0,
      5.0,5.5,6.0,7.0,7.5,8.8,9.5,10.0,11.0,12.0,12.8,13.5,
      14.75,15.0,16.0,17.5,18.5,19.0,20.0,21.4,22.4,23.0,24.0,
      26.0,28.0,31.0,32.0,35.0])

zcrude1    = np.ndarray(37)
zcrude1[:] = 9000.0

zcrude2 = np.array(
     [   0., 500.,1200.,2000.,3800.,4200.,
      5000.,6000.,9000.,7500.,4400.,1950.,
      1900.,1800.,1700.,2200.,1700.,5200.,
      1600.,1400.,3600.,5200.,1000., 800.,
      1800., 800., 600., 400., 350.,2400.,
       400., 350., 300., 250., 200., 100., 50.])

parminover = -2.3
parmaxover = 33.0

 */
private static final List<Double> tcrude1 = Collections.unmodifiableList(Arrays.asList(
    -3.5,-2.5,-2.3,-2.0,-1.4,-1.0,-0.8,-0.5,-0.4,3.0,
    5.0,5.5,6.0,7.0,7.5,8.8,9.5,10.0,11.0,12.0,12.8,13.5,
    14.75,15.0,16.0,17.5,18.5,19.0,20.0,21.4,22.4,23.0,24.0,
    26.0,28.0,31.0,32.0
));

  private static final List<Double> tcrude2 = Collections.unmodifiableList(Arrays.asList(
      -2.5,-2.3,-2.0,-1.4,-1.0,-0.8,-0.5,-0.4,3.0,
      5.0,5.5,6.0,7.0,7.5,8.8,9.5,10.0,11.0,12.0,12.8,13.5,
      14.75,15.0,16.0,17.5,18.5,19.0,20.0,21.4,22.4,23.0,24.0,
      26.0,28.0,31.0,32.0,35.0
  ));

  private static final double zcrude1 = 9000.0;

  private static final List<Double> zcrude2 = Collections.unmodifiableList(Arrays.asList(
      0.0, 500.0,1200.0,2000.0,3800.0,4200.0,
      5000.0,6000.0,9000.0,7500.0,4400.0,1950.0,
      1900.0,1800.0,1700.0,2200.0,1700.0,5200.0,
      1600.0,1400.0,3600.0,5200.0,1000.0, 800.0,
      1800.0, 800.0, 600.0, 400.0, 350.0,2400.0,
      400.0, 350.0, 300.0, 250.0, 200.0, 100.0, 50.0
  ));

  private static final double parminover = -2.3D;
  private static final double parmaxover = 33.0D;
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failures = new LinkedHashSet<>();

    DepthData depthData = new DepthData(cast);

    List<Integer> levels = depthData.getOriglevels();
    List<Double> depths = depthData.getDepths();
    List<Double> temperatures = depthData.getTemperatures();
    for (int i = 0; i < depthData.getnLevels(); i++) {
      double temp = temperatures.get(i);
      double depth = depths.get(i);
      if ((temp < parminover) || (temp> parmaxover)) {
        failures.add(levels.get(i));
      } else {
        for (int j = 0; j < zcrude2.size(); j++){
          if ((temp >= tcrude1.get(j)) && (temp <= tcrude2.get(j)) && (depth <= zcrude1) && (depth >= zcrude2.get(j))){
            failures.add(levels.get(i));
          }
        }
      }

    }
    return failures;
  }

}
