package edu.colorado.cires.wod.iquodqc.check.csiro.wirebreak;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.isProbeTypeXBT;
import static java.lang.Math.abs;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CsiroWireBreakCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "CSIRO_wire_break";
  }

  /*

    """
    Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    t = p.t()

    # initialize qc as a bunch of falses:
    qc = numpy.zeros(len(t.data), dtype=bool)

    # only meaningful for XBT data
    if p.probe_type() != 2:
        return qc

    no_wb = numpy.where( (t >= -2.4) & (t <= 32) & istight(t) )[0]

    if len(no_wb) > 0:
        last_good = no_wb[-1]
        qc = numpy.append(numpy.zeros(last_good + 1, dtype=bool), numpy.ones(len(t) - last_good - 1, dtype=bool))
    else:
        qc = numpy.ones(len(t.data), dtype=bool)

    if numpy.all(qc):
        qc[:] = False

    return qc


    def istight(t, thresh=0.1):
    # given a temperature profile, return an array of bools
    # true = this level is within thresh of both its neighbors
    gaps = numpy.absolute(numpy.diff(t))
    left = numpy.append(gaps,0)
    right = numpy.insert(gaps,0,0)
    return (left<thresh) & (right<thresh)

   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    int index = -1;

    if (isProbeTypeXBT(cast)) {
      Optional<Double> temperature = getTemperature(depths.get(0)).map(v -> v.getValue());
      Optional<Double> nextTemperature = Optional.empty();
      boolean leftThreshold = true;
      boolean rightThreshold = true;
      for (int i = 0; i < depths.size(); i++) {
        if (i < depths.size()-1) {
          nextTemperature = getTemperature(depths.get(i + 1)).map(v -> v.getValue());
          rightThreshold = isThreshold(temperature, nextTemperature);
        } else {
          rightThreshold = true;
        }
        if (leftThreshold && rightThreshold & inRange(temperature)){
          index = i;
        }
        leftThreshold = rightThreshold;
        temperature = nextTemperature;
      }
    }
    if (index > -1){
      for (int i = index + 1; i <  depths.size(); i++) {
        failures.add(i);
      }
    }
    return failures;
  }

  private boolean isThreshold(Optional<Double> temperature, Optional<Double> nextTemperature){
    double threshold = 0.1;
    double x = 0;
    double y = 0;
    if (temperature.isPresent()){
      x = temperature.get();
    }
    if (nextTemperature.isPresent()){
      y = nextTemperature.get();
    }
    return abs(x-y)<threshold;
  }
  private boolean inRange(Optional<Double> temperature){
    if (temperature.isEmpty()){
      return true;
    }
    double t = temperature.get();
    return (t >= -2.4) && (t <= 32);
  }
}
