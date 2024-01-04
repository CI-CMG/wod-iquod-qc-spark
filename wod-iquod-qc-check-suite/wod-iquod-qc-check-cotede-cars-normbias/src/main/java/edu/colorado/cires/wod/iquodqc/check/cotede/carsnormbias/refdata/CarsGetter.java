package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.Stats;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.StatsGetter;
import java.util.Objects;
import ucar.nc2.Variable;

public class CarsGetter extends StatsGetter<Stats> {
  private final Index index;
  
  private static final CarsGetterProperties STATS_GETTER_PROPERTIES = new CarsGetterProperties(
  "mean",
  "std_dev",
  "lat",
  "lon",
  "depth",
      "add_offset",
      "scale_factor"
  );

  public CarsGetter(CarsParameters carsParameters) {
    super(STATS_GETTER_PROPERTIES);
    this.index = new Index(carsParameters.getDataFilePath(), STATS_GETTER_PROPERTIES);
  }

  @Override
  protected float transformValue(float value, Variable variable) {
    double scaleFactor = getAttributeValue(variable, STATS_GETTER_PROPERTIES.getScaleFactor());
    double addOffset = getAttributeValue(variable, STATS_GETTER_PROPERTIES.getAddOffset());
    return (float) ((value * scaleFactor) + addOffset);
  }

  @Override
  protected Index getNcFile(long epochMillisTimestamp) {
    return index;
  }

  @Override
  protected Stats processAdditionalFields(Index index, double depth, double longitude, double latitude, int[] latIndices, int[] lonIndices,
      int[] depthIndices, Stats baseStats) {
    return baseStats;
  }

  private static double getAttributeValue(Variable variable, String attributeName) {
    return (double) Objects.requireNonNull(Objects.requireNonNull(variable.findAttribute(attributeName)).getValue(0));
  }
  
  
}
