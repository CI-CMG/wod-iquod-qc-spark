package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import java.util.stream.IntStream;

public class CoTeDeCarsNormbias {
  private static final String FILE_PATH = "/Users/paytoncain/Desktop/temperature_cars2009a.nc";
  private static final String MEAN_VARIABLE_NAME = "mean";
  private static final String STANDARD_DEVIATION_VARIABLE_NAME = "std_dev";
  private static final String SCALE_FACTOR_ATTRIBUTE_NAME = "scale_factor";
  private static final String ADD_OFFSET_ATTRIBUTE_NAME = "add_offset";
  private static final String DEPTH_VARIABLE_NAME = "depth";
  private static final String LAT_VARIABLE_NAME = "lat";
  private static final String LON_VARIABLE_NAME = "lon";
  public static double[] computeCarsNormbiases(double[] temperatures, double[] depths, double latitude, double longitude, CarsGetter carsGetter) {
    double transformedLongitude = (longitude % 360 + 360) % 360;
    double transformedLatitude = ((latitude + 90) % 180 + 180) % 180 - 90;
    return IntStream.range(0, temperatures.length)
        .mapToDouble(i -> carsGetter.getStats(0, depths[i], transformedLongitude, transformedLatitude)
            .getNormBias(temperatures[i]))
        .toArray();
  }
}
