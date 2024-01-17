package edu.colorado.cires.wod.iquodqc.check.iquodflags;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.GroupCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IquodFlagsCheck extends GroupCastCheck {

  @Override
  public String getName() {
    return CheckNames.IQUOD_FLAGS_CHECK.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return List.of(
        CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName(),
        CheckNames.COMPROMISE_GROUP.getName(),
        CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName()
    );
  }

  @Override
  protected CastCheckResult checkCast(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    CastCheckResult result = super.checkCast(cast, otherTestResults);
    return CastCheckResult.builder(result)
        .withIquodFlags(
            getFlagValues(
                cast,
                getFailedDepths(CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP, otherTestResults),
                getFailedDepths(CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP, otherTestResults),
                getFailedDepths(CheckNames.COMPROMISE_GROUP, otherTestResults)
            )
        ).build();
  }

  private static List<Integer> getFailedDepths(CheckNames checkName, Map<String, ? extends CastCheckResult> otherTestResults) {
    String name = checkName.getName();

    CastCheckResult castCheckResult = Objects.requireNonNull(
        otherTestResults.get(name),
        String.format(
            "Other test results do not contain check dependency: %s",
            name
        )
    );

    return castCheckResult.getFailedDepths();
  }

  private static int getFlagValue(
      Collection<Integer> htprGroup,
      Collection<Integer> lfprGroup,
      Collection<Integer> compGroup,
      int depthIndex
  ) {
    int flagValue = 1;

    if (htprGroup.contains(depthIndex)) {
      flagValue = 2;
    }

    if (compGroup.contains(depthIndex)) {
      flagValue = 3;
    }

    if (lfprGroup.contains(depthIndex)) {
      flagValue = 4;
    }

    return flagValue;
  }

  private static boolean isXBT(Cast cast) {
    return cast.getAttributes().stream()
        .anyMatch(a -> a.getCode() == PROBE_TYPE &&
            Double.valueOf(a.getValue()).equals((double) ProbeTypeConstants.XBT));
  }

  private static List<Integer> getFlagValues(Cast cast, Collection<Integer> htprGroup, Collection<Integer> lfprGroup, Collection<Integer> compGroup) {
    List<Integer> iquodFlags = IntStream.range(0, cast.getDepths().size())
        .boxed()
        .map(i -> getFlagValue(htprGroup, lfprGroup, compGroup, i))
        .collect(Collectors.toList());

    if (isXBT(cast)) {
      int minFlag = Integer.MIN_VALUE;
      for (int i = 0; i < iquodFlags.size(); i++) {
        int value = iquodFlags.get(i);
        if (value > minFlag) {
          minFlag = iquodFlags.get(i);
        }
        if (value < minFlag) {
          iquodFlags.set(i, minFlag);
        }
      }
    }

    return iquodFlags;
  }
}
