package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.locationtech.spatial4j.distance.DistanceUtils;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;

final class LocationInterpolationUtils {

  private static void getDepths(List<DepthAtPosition> positionTemps, NetcdfFile netFile, EtopoDataHolder dataHolder, int minIndexLat, int maxIndexLat,
      int minIndexLon, int maxIndexLon) {
    Array oceanDepthData;
    try {
      oceanDepthData = Objects.requireNonNull(netFile.findVariable("ROSE"))
          .read(new int[]{minIndexLon, minIndexLat}, new int[]{maxIndexLon - minIndexLon + 1, maxIndexLat - minIndexLat + 1});
    } catch (InvalidRangeException | IOException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }

    IndexIterator it = oceanDepthData.getIndexIterator();
    while (it.hasNext()) {
      float depth = it.getFloatNext();
      if (!Float.isNaN(depth)) {
        int realLonIndex = minIndexLon + it.getCurrentCounter()[1];
        int realLatIndex = minIndexLat + it.getCurrentCounter()[2];
        DepthAtPosition dap = new DepthAtPosition(
            realLonIndex,
            realLatIndex,
            depth,
            dataHolder.getLongitudes()[realLonIndex],
            dataHolder.getLatitudes()[realLatIndex]);
        positionTemps.add(dap);
      }
    }
  }

  private static DepthAtPosition getDepthAtLocationIndex(NetcdfFile netFile, EtopoDataHolder dataHolder, int lonIndex, int latIndex) {
    Array oceanDepthData;
    try {
      oceanDepthData = Objects.requireNonNull(netFile.findVariable("ROSE")).read(new int[]{lonIndex, latIndex}, new int[]{1});
    } catch (InvalidRangeException | IOException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }

    float depth = oceanDepthData.getFloat(0);

    return new DepthAtPosition(
        lonIndex,
        latIndex,
        depth,
        dataHolder.getLongitudes()[lonIndex],
        dataHolder.getLatitudes()[latIndex]);
  }


  static OptionalDouble depthInterpolationProcess(NetcdfFile netFile, EtopoDataHolder dataHolder, double longitude, double latitude) {

    int minIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude - 1f);
    int maxIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude + 1f);
    int minIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude - 1f);
    int maxIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude + 1f);

    List<DepthAtPosition> positionTemps = new ArrayList<>();

    getDepths(positionTemps, netFile, dataHolder, minIndexLat, maxIndexLat, minIndexLon, maxIndexLon);

    // near antimeridian
    if (minIndexLon == 0) {
      getDepths(positionTemps, netFile, dataHolder, minIndexLat, maxIndexLat, dataHolder.getLongitudes().length - maxIndexLon - 1,
          dataHolder.getLongitudes().length - 1);
    }
    if (maxIndexLon == dataHolder.getLongitudes().length - 1) {
      getDepths(positionTemps, netFile, dataHolder, minIndexLat, maxIndexLat, 0, dataHolder.getLongitudes().length - 1 - minIndexLon);
    }

    DepthAtPosition nearest = positionTemps.stream().reduce(null, (dap1, dap2) -> {
      dap2.setDistanceM(InterpolationUtils.distanceM(longitude, latitude, dap2.getLongitude(), dap2.getLatitude()));
      if (dap1 == null) {
        return dap2;
      }
      if (dap2.getDistanceM() < dap1.getDistanceM()) {
        return dap2;
      }
      return dap1;
    });

    double rad = DistanceUtils.distHaversineRAD(latitude, longitude, nearest.getLatitude(), nearest.getLongitude());
    if (rad > 0.25) {
      return OptionalDouble.empty();
    }

    //TODO
    return null;

//    DepthAtPosition knot = getDepthAtLocationIndex(netFile, dataHolder, nearest.getLonIndex(), nearest.getLatIndex());
//
//    PolynomialSplineFunction intFunc = new LinearInterpolator().interpolate(
//        knots.stream().mapToDouble(tap -> clipZero ? Math.max(0d, tap.getDepth()) : tap.getDepth()).toArray(),
//        knots.stream().mapToDouble(TempAtPosition::getTemperature).toArray());
//    return interpolate(depth, intFunc);
  }

  private LocationInterpolationUtils() {

  }
}
