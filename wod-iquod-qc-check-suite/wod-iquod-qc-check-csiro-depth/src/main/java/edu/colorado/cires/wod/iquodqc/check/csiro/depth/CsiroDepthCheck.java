package edu.colorado.cires.wod.iquodqc.check.csiro.depth;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.isProbeTypeXBT;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class CsiroDepthCheck extends CommonCastCheck {



  @Override
  public String getName() {
    return "CSIRO_depth_validation";
  }

  /*
     """
    Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    # Get depth values (m) from the profile.
    d = p.z()
    # is this an xbt?
    isXBT = p.probe_type() == 2

    # initialize qc as a bunch of falses;
    qc = numpy.zeros(p.n_levels(), dtype=bool)

    # check for gaps in data
    isDepth = (d.mask==False)

    for i in range(p.n_levels()):
        if isDepth[i]:
            # too-shallow temperatures on XBT probes
            # note we simply flag this profile for manual QC, in order to minimize false negatives.
            if isXBT and d.data[i] < 3.6:
                qc[i] = True

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
//    Optional<Double> probeType = cast.getAttributes().stream()
//        .filter(a -> a.getCode()==PROBE_TYPE).findFirst().map(Attribute::getValue);

//    if (doubleEquals(probeType, Optional.of((double)XBT))){
    if (isProbeTypeXBT(cast)){
      for (int i = 0; i < depths.size(); i++) {
        Depth depth = depths.get(i);
        if (depth.getDepth() < 3.6) {
          failures.add(i);
        }
      }
    }

    return failures;
  }

}
