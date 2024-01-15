package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;

public abstract class GroupCastCheck extends CommonCastCheck {

  @Override
  public abstract String getName();

  @Override
  public abstract Collection<String> dependsOn();

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new NotImplementedException(
        String.format(
            "getFailedDepths not implemented for test: %s", getName()
        )
    );
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    Collection<Integer> failedDepths = new HashSet<>(0);

    for (String dependency : dependsOn()) {
      CastCheckResult castCheckResult = otherTestResults.get(dependency);

      if (castCheckResult == null) {
        throw new IllegalStateException(
            String.format(
                "Could not gather results from test: %s", dependency
            )
        );
      }

      failedDepths.addAll(castCheckResult.getFailedDepths());
    }

    return failedDepths.stream()
        .sorted()
        .collect(Collectors.toList());
  }
}
