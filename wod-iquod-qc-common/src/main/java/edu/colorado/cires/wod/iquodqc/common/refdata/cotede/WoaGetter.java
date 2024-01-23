package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolatingFunction;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolator;
import ucar.nc2.Variable;

public class WoaGetter extends StatsGetter<WoaStats> {

  private TricubicInterpolatingFunction nObservationsInterpolator = null;
  private TricubicInterpolatingFunction standardErrorInterpolator = null;
  private final WoaParameters woaParameters;

  @Override
  protected float transformValue(float value, Variable variable) {
    return value;
  }

  @Override
  protected Index getNcFile(long epochMillisTimestamp) {
    return getNcFile(getSeason(epochMillisTimestamp));
  }

  @Override
  protected WoaStats processAdditionalFields(
      Index index,
      double depth,
      double longitude,
      double latitude,
      int minIndexLat,
      int maxIndexLat,
      int minIndexLon,
      int maxIndexLon,
      Stats baseStats
  ) {
    if (nObservationsInterpolator == null) {
      nObservationsInterpolator = prepareInterpolator(
          getValues(
              STATS_GETTER_PROPERTIES.getNumberOfObservations(),
              index,
              0,
              index.getDepths().length - 1,
              minIndexLat,
              maxIndexLat,
              minIndexLon,
              maxIndexLon
          )
      );
    }

    if (standardErrorInterpolator == null) {
      standardErrorInterpolator = prepareInterpolator(
          getValues(
              STATS_GETTER_PROPERTIES.getStandardError(),
              index,
              0,
              index.getDepths().length - 1,
              minIndexLat,
              maxIndexLat,
              minIndexLon,
              maxIndexLon
          )

      );
    }

    return new WoaStats(
        baseStats.getMean().isEmpty() ? Double.NaN : baseStats.getMean().getAsDouble(),
        baseStats.getStandardDeviation().isEmpty() ? Double.NaN : baseStats.getStandardDeviation().getAsDouble(),
        getStatField(
            depth,
            longitude,
            latitude,
            nObservationsInterpolator
        ),
        getStatField(
            depth,
            longitude,
            latitude,
            standardErrorInterpolator
        )
    );
  }

  private enum Season {
    WINTER,
    SPRING,
    SUMMER,
    FALL
  }

  private Index winterIndex = null;
  private Index springIndex = null;
  private Index summerIndex = null;
  private Index fallIndex = null;

  private static final WoaGetterProperties STATS_GETTER_PROPERTIES = new WoaGetterProperties(
      "t_mn",
      "t_sd",
      "lat",
      "lon",
      "depth",
      "t_dd",
      "t_se"
  );

  public WoaGetter(WoaParameters parameters) {
    super(STATS_GETTER_PROPERTIES);
    woaParameters = parameters;
  }

  private static Season getSeason(long epochMillisTimestamp) {
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisTimestamp), ZoneId.of("UTC"));
    switch (ldt.getMonth()) {
      case JANUARY:
      case FEBRUARY:
      case MARCH:
        return Season.WINTER;
      case APRIL:
      case MAY:
      case JUNE:
        return Season.SPRING;
      case JULY:
      case AUGUST:
      case SEPTEMBER:
        return Season.SUMMER;
      case OCTOBER:
      case NOVEMBER:
      case DECEMBER:
        return Season.FALL;
      default:
        throw new IllegalStateException("Unable to determine season: " + ldt.getMonth());
    }
  }

  private Index getNcFile(Season season) {
    switch (season) {
      case WINTER:
        if (winterIndex == null) {
          winterIndex = new Index(woaParameters.getS1Path(), STATS_GETTER_PROPERTIES);
        }
        return winterIndex;
      case SPRING:
        if (springIndex == null) {
          springIndex = new Index(woaParameters.getS2Path(), STATS_GETTER_PROPERTIES);
        }
        return springIndex;
      case SUMMER:
        if (summerIndex == null) {
          summerIndex = new Index(woaParameters.getS3Path(), STATS_GETTER_PROPERTIES);
        }
        return summerIndex;
      case FALL:
        if (fallIndex == null) {
          fallIndex = new Index(woaParameters.getS4Path(), STATS_GETTER_PROPERTIES);
        }
        return fallIndex;
      default:
        throw new IllegalStateException("Unable to determine season file: " + season);
    }
  }

}
