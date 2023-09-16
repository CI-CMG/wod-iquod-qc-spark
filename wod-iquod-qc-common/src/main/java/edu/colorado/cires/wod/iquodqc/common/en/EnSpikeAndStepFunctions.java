package edu.colorado.cires.wod.iquodqc.common.en;

import java.util.List;
import java.util.Set;

final class EnSpikeAndStepFunctions {

  static double determineDepthTolerance(double z, double latitude) {
    double depthTol;
    if (latitude < 20.0) {
      depthTol = 300.0;
    } else {
      depthTol = 200.0;
    }

    double tTolFactor;
    if (z > 600.0) {
      tTolFactor = 0.3;
    } else if (z > 500.0) {
      tTolFactor = 0.4;
    } else if (z > depthTol + 100.0) {
      tTolFactor = 0.5;
    } else if (z > depthTol) {
      tTolFactor = 1.0 - 0.005 * (z - depthTol);
    } else {
      tTolFactor = 1.0;
    }

    return tTolFactor * 5.0;
  }

  static void conditionA(List<Double> dt, double dTTol, Set<Integer> failures, double wt1, int i, boolean suspect) {
// condition A (large spike check)
    /*
    if (dt.mask[i-1] == False and dt.mask[i] == False and np.max(np.abs(dt[i-1:i+1])) > dTTol):
        if np.abs(dt[i] + dt[i-1]) < 0.5*dTTol:
            dt[i-1:i+1] = np.ma.masked
            if suspect == False: qc[i-1] = True
        elif np.abs((1.0-wt1) * dt[i-1] - wt1*dt[i]) < 0.5*dTTol:
            # Likely to be a valid large temperature gradient.
            dt[i-1:i+1] = np.ma.masked # Stops the levels being rechecked.
     */
    if (
        dt.get(i - 1) != null &&
            dt.get(i) != null &&
            Math.max(Math.abs(dt.get(i - 1)), Math.abs(dt.get(i))) > dTTol
    ) {
      if (Math.abs(dt.get(i) + dt.get(i - 1)) < 0.5 * dTTol) {
        dt.set(i - 1, null);
        dt.set(i, null);
        if (!suspect) {
          failures.add(i - 1);
        }
      } else if (Math.abs((1.0 - wt1) * dt.get(i - 1) - wt1 * dt.get(i)) < 0.5 * dTTol) {
        dt.set(i - 1, null);
        dt.set(i, null);
      }
    }
  }

  static void conditionB(List<Double> dt, double dTTol, double gTTol, Set<Integer> failures, int i, boolean suspect, List<Double> gt) {
    /*
        '''
    condition B (small spike check)
    '''
    if (dt.mask[i-1] == False and dt.mask[i] == False and
        np.max(np.abs(dt[i-1:i+1])) > 0.5*dTTol and
        np.max(np.abs(gt[i-1:i+1])) > gTTol and
        np.abs(dt[i] + dt[i-1]) < 0.25*np.abs(dt[i] - dt[i-1])):
        dt[i-1:i+1] = np.ma.masked
        if suspect == False: qc[i-1] = True

    return qc, dt
     */
    if (
        dt.get(i - 1) != null &&
            dt.get(i) != null &&
            gt.get(i - 1) != null &&
            gt.get(i) != null &&
            Math.max(Math.abs(dt.get(i - 1)), Math.abs(dt.get(i))) > 0.5 * dTTol &&
            Math.max(Math.abs(gt.get(i - 1)), Math.abs(gt.get(i))) > gTTol &&
            Math.abs(dt.get(i) + dt.get(i - 1)) < 0.25 * Math.abs(dt.get(i) - dt.get(i - 1))
    ) {
      dt.set(i - 1, null);
      dt.set(i, null);
      if (!suspect) {
        failures.add(i - 1);
      }
    }
  }

  private static double interpolate(double depth, double shallow, double deep, double shallowVal, double deepVal) {
//    '' '
//    interpolate values at<depth>
//    '' '
    return (depth - shallow) / (deep - shallow) * (deepVal - shallowVal) + shallowVal;
  }

  static void conditionC(List<Double> dt, double dTTol, double[] z, Set<Integer> failures, List<Double> t, int i, boolean suspect) {
    /*
        '''
    condition C (steps)
    '''

    if dt.mask[i-1] == False and np.abs(dt[i-1]) > dTTol:
        if z[i-1] <= 250.0 and dt[i-1] < -dTTol and dt[i-1] > -3.0*dTTol:
            # May be sharp thermocline, do not reject.
            pass
        elif i>1 and z[i] - z[i-2] > 0 and np.abs(t[i-1] - interpolate(z[i-1], z[i-2], z[i], t[i-2], t[i])) < 0.5*dTTol:
            # consistent interpolation, do not reject
            pass
        else:
            # mark both sides of the step
            if suspect == True: qc[i-2:i] = True
     */
    if (dt.get(i - 1) != null && Math.abs(dt.get(i - 1)) > dTTol && t.get(i - 1) != null && t.get(i) != null) {
      if (
          suspect &&
              !(z[i - 1] <= 250.0 && dt.get(i - 1) < -dTTol && dt.get(i - 1) > -3.0 * dTTol) &&
              !(i > 1 && z[i] - z[i - 2] > 0D && Math.abs(t.get(i - 1) - interpolate(z[i - 1], z[i - 2], z[i], t.get(i - 2), t.get(i))) < 0.5 * dTTol)
      ) {
        failures.add(i - 2);
        failures.add(i - 1);
      }
    }
  }

  private EnSpikeAndStepFunctions() {

  }
}
