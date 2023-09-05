package edu.colorado.cires.wod.iquodqc.check.csiro.shortgradient;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.XBT;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;
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

public class CsiroShortGradientCheck extends CommonCastCheck {



  @Override
  public String getName() {
    return "CSIRO_short_gradient";
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

    for i in range(0, p.n_levels()-1 ):
        if isData[i] and isData[i+1]:
            deltaD = (d.data[i+1] - d.data[i])
            deltaT = (t.data[i+1] - t.data[i])
            if deltaT == 0:
                continue
            gradshort = deltaD / deltaT
            if (deltaT > 0.5 and deltaD < 30) or abs(gradshort) < 0.4 or (gradshort > 0 and gradshort < 12.5):
                if abs(deltaT) > 0.4:
                    qc[i] = True

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    for (int i = 0; i < depths.size()-1; i++) {
      Optional<Double> temp1 = getTemperature(depths.get(i)).map(v -> v.getValue());
      Optional<Double> temp2 = getTemperature(depths.get(i+1)).map(v -> v.getValue());

      if (temp1.isPresent() && temp2.isPresent()) {
        double deltaD = depths.get(i+1).getDepth() - depths.get(i).getDepth();
        double deltaT = temp2.get() - temp1.get();
        if (doubleEquals(Optional.of((double)deltaT),Optional.of((double)0))){
          continue;
        }
        double gradshort = deltaD / deltaT;
        if(((deltaT > 0.5D && deltaD <30D) || (abs(gradshort) < 0.4D) || (gradshort > 0D && gradshort < 12.5D)) &&
            (abs(deltaT) > 0.4D)){
            failures.add(i);
        }
      }

    }
    return failures;
  }

}
