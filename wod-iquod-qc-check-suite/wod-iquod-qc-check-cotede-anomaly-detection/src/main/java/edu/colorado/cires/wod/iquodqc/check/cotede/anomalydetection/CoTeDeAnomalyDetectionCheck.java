package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.DependsOnSignalCastCheck;
import edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H.CoTeDeTukey53H;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CoTeDeAnomalyDetectionCheck extends DependsOnSignalCastCheck {

  private static final double THRESHOLD = -18.0;

  @Override
  public String getName() {
    return CheckNames.COTEDE_ANOMALY_DETECTION_CHECK.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return List.of(
        CheckNames.COTEDE_GRADIENT_CHECK.getName(),
        CheckNames.COTEDE_SPIKE_CHECK.getName(),
        CheckNames.COTEDE_RATE_OF_CHANGE.getName(),
        CheckNames.COTEDE_WOA_NORMBIAS.getName(),
        CheckNames.COTEDE_CARS_NORMBIAS_CHECK.getName(),
        CheckNames.COTEDE_CONSTANT_CLUSTER_SIZE_CHECK.getName()
    );
  }

  @Override
  protected Collection<Integer> getFailedDepthsFromSignals(Cast cast, Map<String, List<Double>> signals) {
    return Arrays.stream(CoTeDeAnomalyDetection.getFlags(
        getSignal(CheckNames.COTEDE_GRADIENT_CHECK.getName(), signals),
        getSignal(CheckNames.COTEDE_SPIKE_CHECK.getName(), signals),
        getSignal(
            CheckNames.COTEDE_TUKEY_53_NORM_CHECK.getName(),
            Map.of(
                CheckNames.COTEDE_TUKEY_53_NORM_CHECK.getName(),
                Arrays.stream(CoTeDeTukey53H.computeTukey53H(getTemperatures(cast), true))
                    .boxed().collect(Collectors.toList())
            )
        ),
        getSignal(CheckNames.COTEDE_RATE_OF_CHANGE.getName(), signals),
        getSignal(CheckNames.COTEDE_WOA_NORMBIAS.getName(), signals),
        getSignal(CheckNames.COTEDE_CARS_NORMBIAS_CHECK.getName(), signals),
        getSignal(CheckNames.COTEDE_CONSTANT_CLUSTER_SIZE_CHECK.getName(), signals),
        THRESHOLD
    )).boxed().collect(Collectors.toList());
  }

  private static double[] getSignal(String checkName, Map<String, List<Double>> signals) {
    List<Double> signal = Objects.requireNonNull(
        signals.get(checkName),
        String.format(
            "Signal not set for check: %s",
            checkName
        )
    );

    return signal.stream()
        .mapToDouble(Double::doubleValue)
        .toArray();
  }
}
