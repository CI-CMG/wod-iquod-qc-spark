package edu.colorado.cires.wod.iquodqc.check.icdcaqc04.maxobsdepth;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getProbeTypeValue;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

public class IcdcAqc04MaxObsDepthCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "ICDC_aqc_04_max_obs_depth";
  }
/*
      '''Return quality control decisions.
          '''

          # Get WOD database.
      db = wod_database(p)

    # Set maximum allowed depth. If not defined, it is set to the highest
    # possible float.
      if db == 'OSD':
  zlast = 9000.0
  elif db == 'CTD':
  zlast = 9000.0
  elif db == 'PFL':
  zlast = 2020.0
  elif db == 'APB':
  zlast = 1200.0
  elif db == 'MBT' and p.primary_header['Country code'] == 'JP':
  zlast = 295.00001
  elif db == 'XBT':
  zlast = 1900.0
      else:
  zlast = np.finfo(dtype=float).max

    # Set QC flags.
      qc = p.z() >= zlast

    return qc

 */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    OptionalInt probeType = getProbeTypeValue(cast);
    double zlast = Double.MAX_VALUE;
    if (probeType.equals(OptionalInt.of(ProbeTypeConstants.STD)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.XCTD)) ||
        probeType.equals(OptionalInt.of(ProbeTypeConstants.BOTTLE_ROSSETTE_NET)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.BUCKET))){
      zlast = 9000.0;
    }else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.CTD))){
      zlast = 9000.0;
    }else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.PROFILING_FLOAT))){
      zlast = 2020.0;
    }else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.ANIMAL_MOUNTED))){
      zlast = 1200.0;
    }else if ((probeType.equals(OptionalInt.of(ProbeTypeConstants.MBT)) || probeType.equals(OptionalInt.of(ProbeTypeConstants.DBT))||probeType.equals(OptionalInt.of(ProbeTypeConstants.MICRO_BT)))
        && cast.getCountry().equals("JP")){
      zlast = 295.00001;
    }else if (probeType.equals(OptionalInt.of(ProbeTypeConstants.XBT))) {
      zlast = 1900.0;
    }else {
      return failures;
    }
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      if (depth.getDepth() >= zlast) {
        failures.add(i);
      }
    }
    return failures;
  }

}
