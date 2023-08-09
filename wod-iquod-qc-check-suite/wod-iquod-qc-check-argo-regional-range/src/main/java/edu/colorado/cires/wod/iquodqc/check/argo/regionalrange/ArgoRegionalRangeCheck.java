package edu.colorado.cires.wod.iquodqc.check.argo.regionalrange;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

public class ArgoRegionalRangeCheck extends CommonCastCheck {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

  private static final Polygon RED_SEA = GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
      new CoordinateXY(40D, 10D), new CoordinateXY(50D, 20D), new CoordinateXY(30D, 30D), new CoordinateXY(40D, 10D)
  });

  private static final Polygon MEDITERRANEAN = GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
      new CoordinateXY(6D, 30D),
      new CoordinateXY(40D, 30D),
      new CoordinateXY(35D, 40D),
      new CoordinateXY(20D, 42D),
      new CoordinateXY(15D, 50D),
      new CoordinateXY(5D, 40D),
      new CoordinateXY(6D, 30D)
  });


  @Override
  public String getName() {
    return "Argo_regional_range_test";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    Point point = GEOMETRY_FACTORY.createPoint(new CoordinateXY(cast.getLongitude(), cast.getLatitude()));

    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);

      boolean failed = getTemperature(depth)
          .map(ProfileData::getValue)
          .map(temp -> {
            if (point.intersects(RED_SEA)) {
              return temp < 21.7 || temp > 40D;
            }
            if (point.intersects(MEDITERRANEAN)) {
              return temp < 10D || temp > 40D;
            }
            return false;
          }).orElse(false);

      if(failed) {
        failures.add(i);
      }

    }
    return failures;
  }

}
