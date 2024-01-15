package edu.colorado.cires.wod.iquodqc.check.icdcaqc05.stuckvalue;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getProbeTypeValue;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

public class IcdcAqc05StuckValueCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ICDC_AQC_05_STUCK_VALUE.getName();
  }
/*
      '''Return quality control decisions.
    '''

    # Default set of QC flags to return.
    qc = np.zeros(p.n_levels(), dtype=bool)

    # Set minimum allowed levels.
    db = wod_database(p)
    if db == 'OSD':
        minlevs = 7
    elif db == 'CTD':
        minlevs = 50
    elif db == 'PFL':
        minlevs = 20
    elif db == 'APB':
        minlevs = 20
    elif db == 'MBT':
        minlevs = 7
    elif db == 'XBT':
        minlevs = 20
    else:
        return qc # Do not have the information to QC other types.

    # Check that we have the levels we need.
    nlevels, z, t = ICDC.reordered_data(p, data_store)
    if nlevels <= minlevs: return qc

    # Count stuck values.
    n = np.ones(nlevels, dtype=int)
    for i in range(nlevels - minlevs):
        for j in range(i + 1, nlevels):
            diff = np.abs(t[i] - t[j])
            if diff > 0.0001: break
            n[i] += 1

    # Find the largest stuck value range.
    i = np.argmax(n)
    if n[i] < minlevs: return qc
    thick = z[i + n[i] - 1] - z[i]
    if thick >= 200.0:
        # If setting the QC flags we need to be careful about level order.
        qclo = qc[0:nlevels]
        qclo[i:i+n[i]] = True
        qc = ICDC.revert_qc_order(p, qclo, data_store)

    return qc

 */

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failures = new LinkedHashSet<>();

    OptionalInt probeType = getProbeTypeValue(cast);
    int minlevs = 7;
    if (probeType.equals(OptionalInt.of(ProbeTypeConstants.STD)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.XCTD)) ||
        probeType.equals(OptionalInt.of(ProbeTypeConstants.BOTTLE_ROSSETTE_NET)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.BUCKET))) {
      // OSD
      minlevs = 7;
    } else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.CTD))) {
      //CTD
      minlevs = 50;
    } else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.PROFILING_FLOAT))) {
      // PFL
      minlevs = 20;
    } else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.ANIMAL_MOUNTED))) {
      // APB
      minlevs = 20;
    } else if ((probeType.equals(OptionalInt.of(ProbeTypeConstants.MBT)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.DBT))
        || probeType.equals(OptionalInt.of(ProbeTypeConstants.MICRO_BT)))) {
      // MBT
      minlevs = 7;
    } else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.XBT))) {
      // XBT
      minlevs = 20;
    } else {
      return failures;
    }

    DepthData depthData = new DepthData(cast);

    if (depthData.getnLevels() <= minlevs) {
      return failures;
    }
    List<Integer> levels = depthData.getOriglevels();
    List<Double> depths = depthData.getDepths();
    List<Double> temperatures = depthData.getTemperatures();

    int nlevels = depthData.getnLevels();
    Integer[] n = new Integer[nlevels];
    Arrays.fill(n, 1);

    for (int i = 0; i < nlevels - minlevs; i++) {
      double tempI = temperatures.get(i);
      for (int j = i + 1; j < nlevels; j++) {
        if (Math.abs(tempI - temperatures.get(j)) > 0.001) {
          break;
        }
        n[i] = n[i] + 1;
      }
    }

    List<Integer> list = Arrays.asList(n);
    int maxIndex = IntStream.range(0, list.size())
        .reduce((i, j) -> list.get(i) > list.get(j) ? i : j)
        .getAsInt();

    if (n[maxIndex] < minlevs) {
      return failures;
    }

    int last = maxIndex + n[maxIndex];
    double thick = depths.get(last - 1) - depths.get(maxIndex);
    if (thick >= 200.0) {
      for (int i = maxIndex; i < last; i++) {
        failures.add(levels.get(i));
      }
    }
    return failures;
  }

}
