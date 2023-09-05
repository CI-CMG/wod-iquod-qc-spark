package edu.colorado.cires.wod.iquodqc.check.csiro.longgradient;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static java.lang.Math.abs;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CsiroLongGradientCheck extends CommonCastCheck {



  @Override
  public String getName() {
    return "CSIRO_long_gradient";
  }

  /*
     """
    Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    # depths
    d = p.z()
    # temperatures
    t = p.t()

    # initialize qc as a bunch of falses;
    qc = numpy.zeros(len(t.data), dtype=bool)

    # check for gaps in data
    isDepth = (d.mask==False)
    isTemperature = (t.mask==False)
    isData = isTemperature & isDepth

    on_inv = False # are we currently in an inversion?

    for i in range(0, p.n_levels()-1 ):
        if isData[i] and isData[i+1]:
            # not interested below 5m:
            if d.data[i] < 5: continue

            if t.data[i+1] > t.data[i] and not on_inv:
                # entering an inversion
                start_inv_temp = t.data[i]
                start_inv_depth = d.data[i]
                potential_flag = i
                on_inv = True

            if t.data[i+1] < t.data[i] and on_inv:
                # exiting the inversion
                end_inv_temp = t.data[i]
                end_inv_depth = d.data[i]
                on_inv = False
                gradlong = (end_inv_depth - start_inv_depth) / (end_inv_temp - start_inv_temp)

                if abs(gradlong) < 4:
                    qc[potential_flag] = True

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    boolean on_inv = false;
    double start_inv_temp = 0D;
    double end_inv_temp = 0D;
    double start_inv_depth = 0D;
    double end_inv_depth = 0D;
    int potential_flag = -1;

    for (int i = 0; i < depths.size()-1; i++) {
      double depth1 = depths.get(i).getDepth();
      Optional<Double> temp1 = getTemperature(depths.get(i)).map(v -> v.getValue());
      Optional<Double> temp2 = getTemperature(depths.get(i+1)).map(v -> v.getValue());

      if (depth1 < 5D || temp1.isEmpty() || temp2.isEmpty()) {
        continue;
      }

      if (temp2.get() > temp1.get() && !on_inv) {
        // entering an inversion
        start_inv_temp = temp1.get();
        start_inv_depth = depth1;
        potential_flag = i;
        on_inv = true;
      }

      if (temp2.get() < temp1.get() && on_inv) {
//        exiting the inversion
        end_inv_temp = temp1.get();
        end_inv_depth = depth1;
        on_inv = false;
        double gradlong = (end_inv_depth - start_inv_depth) / (end_inv_temp - start_inv_temp);

        if (abs(gradlong) < 4D) {
          failures.add(potential_flag);
        }
      }
    }
    return failures;
  }

}
