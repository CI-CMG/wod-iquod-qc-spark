package edu.colorado.cires.wod.iquodqc.check.argo.gradient;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DoubleUtils;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ArgoGradientCheck extends CommonCastCheck {
  @Override
  public String getName() {
    return "Argo_gradient_test";
  }


  /*
  Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    # Get temperature values from the profile.
    t = p.t()
    # Get depth values (m) from the profile.
    z = obs_utils.depth_to_pressure(p.z(), p.latitude())

    assert len(t.data) == len(z.data), 'Number of temperature measurements does not equal number of depth measurements.'

    # initialize qc as a bunch of falses;
    # implies all measurements pass when a gradient can't be calculated, such as at edges & gaps in data:
    qc = numpy.zeros(len(t.data), dtype=bool)

    # check for gaps in data
    isTemperature = (t.mask==False)
    isPressure = (z.mask==False)
    isData = isTemperature & isPressure

    for i in range(1,len(t.data)-1):
        if isData[i] & isTemperature[i-1] & isTemperature[i+1]:

          isSlope = numpy.abs(t.data[i] - (t.data[i-1] + t.data[i+1])/2)

          if z.data[i] < 500:
              qc[i] = isSlope > 9.0
          else:
              qc[i] = isSlope > 3.0

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    double latitude = cast.getLatitude();

    for (int i = 1; i < depths.size()-1; i++) {
      Optional<Double> prevT= getTemperature(depths.get(i-1)).map(v -> v.getValue());
      Optional<Double> currentT= getTemperature(depths.get(i)).map(v -> v.getValue());
      Optional<Double> nextT= getTemperature(depths.get(i+1)).map(v -> v.getValue());
      if (prevT.isPresent() && currentT.isPresent() && nextT.isPresent()) {
        double pressure = ObsUtils.depthToPressure(depths.get(i).getDepth(),latitude);
        double slope = Math.abs(currentT.get() - (prevT.get() + nextT.get())/2);
        if (pressure < 500){
          if (slope > 9.0){
            failures.add(i);
          }
        } else {
          if (slope > 3.0){
            failures.add(i);
          }
        }
      }
    }
    return failures;
  }
}
