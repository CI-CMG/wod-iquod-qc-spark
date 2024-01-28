package edu.colorado.cires.wod.iquodqc.check.cotede.constantclustersize;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class CoTeDeConstantClusterSizeCheck extends SignalProducingCastCheck {

  @Override
  public String getName() {
    return CheckNames.COTEDE_CONSTANT_CLUSTER_SIZE_CHECK.getName();
  }

  /*
  This test is not intended to contribute to IQUoD flags, this value should remain empty. This test is only intended to provide
  a signal to tests that depend on it
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    signal = Arrays.stream(ConstantClusterSize.computeClusterSizes(getTemperatures(cast), 0)).boxed()
        .map(v -> (double) v)
        .collect(Collectors.toList());
    return Collections.emptyList();
  }
}
