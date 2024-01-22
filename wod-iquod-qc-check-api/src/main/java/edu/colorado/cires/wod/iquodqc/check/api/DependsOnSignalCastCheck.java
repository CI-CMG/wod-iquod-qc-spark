package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;

public abstract class DependsOnSignalCastCheck extends CommonCastCheck {


  @Override
  public abstract String getName();

  protected abstract Collection<Integer> getFailedDepthsFromSignals(Cast cast, Map<String, List<Double>> signals);

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new NotImplementedException(
        String.format("getFailedDepths(cast) not implemented for check: %s", getName())
    );
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return getFailedDepthsFromSignals(
        cast,
        getDependsOnSignals(cast.getDepths().size(), otherTestResults)
    );
  }

  private static Map<String, List<Double>> getDependsOnSignals(int numberOfDepths, Map<String, CastCheckResult> otherTestResults) {
    Map<String, List<Double>> signals = new HashMap<>(0);

    for (Map.Entry<String, CastCheckResult> entry : otherTestResults.entrySet()) {
      String test = entry.getKey();

      CastCheckResult testResult = Objects.requireNonNull(
          entry.getValue(),
          String.format("No CastCheckResult available for test: %s", test)
      );

      List<Double> signal = Objects.requireNonNull(
          testResult.getSignal(),
          String.format("Signal from test must not be null: %s", test)
      );

      int signalSize = signal.size();
      if (signalSize != numberOfDepths) {
        throw new IllegalStateException(
            String.format(
                "Length of %s signal does not match desired number of depths. Wanted: %s, Received: %s",
                test,
                numberOfDepths,
                signalSize
            )
        );
      }

      signals.put(
          test,
          signal
      );
    }

    return signals;
  }
}
