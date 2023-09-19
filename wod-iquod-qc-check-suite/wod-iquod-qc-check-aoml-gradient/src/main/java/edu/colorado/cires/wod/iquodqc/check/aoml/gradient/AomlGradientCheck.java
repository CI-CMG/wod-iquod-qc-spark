package edu.colorado.cires.wod.iquodqc.check.aoml.gradient;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.math3.util.Precision;

public class AomlGradientCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.AOML_GRADIENT.getName();
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();

    Set<Integer> failed = new LinkedHashSet<>();
    for (int i = 0; i < depths.size() - 1; i++) {
      Depth d1 = depths.get(i);
      Depth d2 = depths.get(i + 1);
      // TODO will depth ever be null?
//      if (getTemperature(d1).isPresent() && getTemperature(d2).isPresent() && d1.getDepth() != null && d2.getDepth() != null) {
      Optional<ProfileData> d1Temperature = DepthUtils.getTemperature(d1);
      Optional<ProfileData> d2Temperature = DepthUtils.getTemperature(d2);

      if (d1Temperature.isPresent() && d2Temperature.isPresent()) {
        double dz = d2.getDepth() - d1.getDepth();
        if (Precision.equals(0D, dz)) {
          continue;
        }
        double td = d2Temperature.get().getValue() - d1Temperature.get().getValue();
        double gradTest = td / dz;
        if (td < 0D) {
          if (Math.abs(gradTest) > 1.0) {
            failed.add(i);
            failed.add(i + 1);
          }
        } else if (gradTest > 0.2) {
          failed.add(i);
          failed.add(i + 1);
        }

      }

    }
    return failed;
  }

}
