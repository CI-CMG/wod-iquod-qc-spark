package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import ucar.nc2.Variable;

public class WoaGetter extends StatsGetter<WoaStats> {

  @Override
  protected float transformValue(float value, Variable variable) {
    return value;
  }

  @Override
  protected Index getNcFile(long epochMillisTimestamp) {
    return getNcFile(getSeason(epochMillisTimestamp));
  }

  @Override
  protected WoaStats processAdditionalFields(Index index, double depth, double longitude, double latitude, int[] latIndices, int[] lonIndices,
      int[] depthIndices, Stats baseStats) {
    return new WoaStats(
        baseStats.getMean().isEmpty() ? Double.NaN : baseStats.getMean().getAsDouble(),
        baseStats.getStandardDeviation().isEmpty() ? Double.NaN : baseStats.getStandardDeviation().getAsDouble(),
        getStatField(
            STATS_GETTER_PROPERTIES.getNumberOfObservations(), 
            index,
            depth,
            longitude,
            latitude,
            latIndices,
            lonIndices,
            depthIndices
        ),
        getStatField(
            STATS_GETTER_PROPERTIES.getStandardError(), 
            index,
            depth,
            longitude,
            latitude,
            latIndices,
            lonIndices,
            depthIndices
        )
    );
  }

  private enum Season {
    WINTER,
    SPRING,
    SUMMER,
    FALL
  }

  private final Index winterIndex;
  private final Index springIndex;
  private final Index summerIndex;
  private final Index fallIndex;
  
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
    winterIndex = new Index(parameters.getS1Path(), STATS_GETTER_PROPERTIES);
    springIndex = new Index(parameters.getS2Path(), STATS_GETTER_PROPERTIES);
    summerIndex = new Index(parameters.getS3Path(), STATS_GETTER_PROPERTIES);
    fallIndex = new Index(parameters.getS4Path(), STATS_GETTER_PROPERTIES);
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
        return winterIndex;
      case SPRING:
        return springIndex;
      case SUMMER:
        return summerIndex;
      case FALL:
        return fallIndex;
      default:
        throw new IllegalStateException("Unable to determine season file: " + season);
    }
  }

}
