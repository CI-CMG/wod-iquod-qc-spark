package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.util.FastMath;

public class CoTeDeWoaNormbias {

  public static List<NormBias> computeNormBiases(Cast cast, WoaGetter woaGetter) {
    return cast.getDepths().stream()
        .map(d -> computeNormBias(
            cast.getTimestamp(),
            cast.getLongitude(),
            cast.getLatitude(),
            d,
            woaGetter
        ))
        .collect(Collectors.toList());
  }

  public static double[] computeNormBiases(long timestamp, double longitude, double latitude, double[] depths, double[] temperatures,
      WoaGetter woaGetter) {
    return IntStream.range(0, depths.length)
        .mapToDouble(i -> {
          WoaStats stats = woaGetter.getStats(timestamp, depths[i], longitude, latitude);

          OptionalDouble maybeMean = stats.getMean();
          OptionalDouble maybeStandardDeviation = stats.getStandardDeviation();
          OptionalInt maybeNumberOfObservations = stats.getNumberOfObservations();
          if (maybeMean.isEmpty() || maybeStandardDeviation.isEmpty() || maybeNumberOfObservations.isEmpty()) {
            return Double.NaN;
          }

          return (temperatures[i] - maybeMean.getAsDouble()) / maybeStandardDeviation.getAsDouble();
        }).toArray();
  }

  private static NormBias computeNormBias(long timestamp, double longitude, double latitude, Depth depth, WoaGetter woaGetter) {
    WoaStats stats = woaGetter.getStats(timestamp, depth.getDepth(), longitude, latitude);
    OptionalDouble maybeMean = stats.getMean();
    OptionalDouble maybeStandardDeviation = stats.getStandardDeviation();
    OptionalInt maybeNumberOfObservations = stats.getNumberOfObservations();
    if (maybeMean.isEmpty() || maybeStandardDeviation.isEmpty() || maybeNumberOfObservations.isEmpty()) {
      return new NormBias(Double.NaN, false, 0);
    }

    Optional<ProfileData> maybeProfileData = getTemperature(depth);
    return maybeProfileData.map(profileData -> new NormBias(
        FastMath.abs((profileData.getValue() - maybeMean.getAsDouble()) / maybeStandardDeviation.getAsDouble()),
        true,
        maybeNumberOfObservations.getAsInt()
    )).orElseGet(() -> new NormBias(Double.NaN, false, 0));

  }

  public static class NormBias {

    private final double value;
    private final boolean valid;
    private final int numberOfObservations;

    public NormBias(double value, boolean valid, int numberOfObservations) {
      this.value = value;
      this.valid = valid;
      this.numberOfObservations = numberOfObservations;
    }

    public double getValue() {
      return value;
    }

    public boolean isValid() {
      return valid;
    }

    public int getNumberOfObservations() {
      return numberOfObservations;
    }
  }

}
