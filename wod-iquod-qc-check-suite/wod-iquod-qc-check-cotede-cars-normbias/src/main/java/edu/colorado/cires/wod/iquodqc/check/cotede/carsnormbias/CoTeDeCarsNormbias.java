package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import java.util.stream.IntStream;

public class CoTeDeCarsNormbias {

  public static double[] computeCarsNormbiases(double[] temperatures, double[] depths, double latitude, double longitude, CarsGetter carsGetter) {
    double transformedLongitude = (longitude % 360 + 360) % 360;
    double transformedLatitude = ((latitude + 90) % 180 + 180) % 180 - 90;
    return IntStream.range(0, temperatures.length)
        .mapToDouble(i -> carsGetter.getStats(0, depths[i], transformedLongitude, transformedLatitude)
            .getNormBias(temperatures[i]))
        .toArray();
  }
}
