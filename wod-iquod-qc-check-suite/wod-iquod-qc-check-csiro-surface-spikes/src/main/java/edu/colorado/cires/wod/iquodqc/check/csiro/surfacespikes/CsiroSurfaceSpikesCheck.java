package edu.colorado.cires.wod.iquodqc.check.csiro.surfacespikes;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.XBT;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static java.lang.Math.abs;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CsiroSurfaceSpikesCheck extends CommonCastCheck {



  @Override
  public String getName() {
    return "CSIRO_surface_spikes";
  }

  /*
    """
    Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    # depths
    d = p.z()
    # is this an xbt?
    isXBT = p.probe_type() == 2

    # initialize qc as a bunch of falses;
    qc = numpy.zeros(len(d.data), dtype=bool)

    # check for gaps in data
    isDepth = (d.mask==False)

    if not isXBT:
        return qc

    # flag any level that is shallower than 4m and is followed by a level shallower than 8m.
    for i in range(p.n_levels()):
        if isDepth[i]:
            if d.data[i] < 4 and i < p.n_levels()-1: #only interested in depths less than 4m and not at the bottom of the profile.
                if d.data[i+1] < 8:
                    qc[i] = True
            else:
                break

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    Optional<Double> probeType = cast.getAttributes().stream()
        .filter(a -> a.getCode()==PROBE_TYPE).findFirst().map(Attribute::getValue);

    if (doubleEquals(probeType, Optional.of((double)XBT))){
      for (int i = 0; i < depths.size()-1; i++) {
        if (depths.get(i).getDepth() < 4D && (depths.get(i+1).getDepth() < 8D)) {
          failures.add(i);
        }
      }
    }

    return failures;
  }

}
