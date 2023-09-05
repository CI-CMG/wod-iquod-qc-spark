package edu.colorado.cires.wod.iquodqc.check.csiro.constantbottom;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.XBT;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import org.apache.commons.math3.util.Precision;

public class CsiroConstantBottomCheck extends CommonCastCheck {



  @Override
  public String getName() {
    return "CSIRO_constant_bottom";
  }

  /*
     """
    Runs the quality control check on profile p and returns a numpy array
    of quality control decisions with False where the data value has
    passed the check and True where it failed.
    """

    # Get temperature values from the profile.
    t = p.t()
    # depths
    d = p.z()
    # is this an xbt?
    isXBT = p.probe_type() == 2
    latitude = p.latitude()

    # initialize qc as a bunch of falses;
    qc = numpy.zeros(len(t.data), dtype=bool)

    # check for gaps in data
    isTemperature = (t.mask==False)
    isDepth = (d.mask==False)
    isData = isTemperature & isDepth

    # need more than one level
    if len(isData) < 2:
        return qc

    # constant temperature at bottom of profile, for latitude > -40 and bottom two depths at least 30m apart:
    if isData[-1] and isData[-2] and isXBT:
        if t.data[-1] == t.data[-2] and latitude > -40 and d.data[-1] - d.data[-2] > 30:
            qc[-1] = True

    return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    if (depths.size() < 2){
      return failures;
    }
    Optional<Double> temp1 = getTemperature(depths.get(depths.size()-1))
        .map(v-> v.getValue());

    Optional<Double> temp2 = getTemperature(depths.get(depths.size()-2))
        .map(v-> v.getValue());

    Optional<Double> probeType = cast.getAttributes().stream()
        .filter(a -> a.getCode()==PROBE_TYPE).findFirst().map(Attribute::getValue);

    if (doubleEquals(probeType, Optional.of((double)XBT))  &&
        doubleEquals(temp1, temp2) && cast.getLatitude() > -40  &&
        depths.get(depths.size()-1).getDepth() - depths.get(depths.size()-2).getDepth() > 30){
        failures.add(depths.size()-1);
    }

    return failures;
  }
}
