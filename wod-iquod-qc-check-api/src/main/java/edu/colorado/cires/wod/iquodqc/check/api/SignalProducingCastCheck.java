package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;

public abstract class SignalProducingCastCheck extends CommonCastCheck {

  private static final long serialVersionUID = 0L;

  protected abstract List<Double> produceSignal(Cast cast, Map<String, CastCheckResult> otherTestResults);

  protected abstract Collection<Integer> getFailedDepths(
      Cast cast, List<Double> signal, Map<String, CastCheckResult> otherTestResults
  );

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new NotImplementedException(
        String.format("getFailedDepths(cat) not implemented for check: %s", getName())
    );
  }

  @Override
  protected CastCheckResult checkCast(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    List<Double> signal = Objects.requireNonNull(
        produceSignal(cast, otherTestResults),
        String.format("signal result must not be null for check %s", getName())
    );

    Collection<Integer> failed = getFailedDepths(cast, signal, otherTestResults);

    return CastCheckResult.builder()
        .withCastNumber(cast.getCastNumber())
        .withPassed(failed.isEmpty())
        .withFailedDepths(new ArrayList<>(failed))
        .withDependsOnFailedDepths(
            CommonCastCheck.getDependsOnFailedDepths(otherTestResults)
        ).withDependsOnFailedChecks(
            CommonCastCheck.getDependsOnFailedChecks(otherTestResults)
        ).withSignal(signal)
        .build();
  }
}