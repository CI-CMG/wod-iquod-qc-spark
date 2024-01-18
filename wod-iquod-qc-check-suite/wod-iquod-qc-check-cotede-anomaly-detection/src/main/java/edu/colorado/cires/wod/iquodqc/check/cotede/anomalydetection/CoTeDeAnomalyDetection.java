package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.CoTeDeCarsNormbias;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.constantclustersize.ConstantClusterSize;
import edu.colorado.cires.wod.iquodqc.check.cotede.gradient.CoTeDeGradient;
import edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange.CoTeDeRateOfChange;
import edu.colorado.cires.wod.iquodqc.check.cotede.spike.CoTeDeSpike;
import edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H.CoTeDeTukey53H;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CoTeDeAnomalyDetection {

  public static List<Integer> checkFlags(
      double[] temperature, double[] depths, long timestamp, double latitude, double longitude, WoaGetter woaGetter, CarsGetter carsGetter, double threshold
  ) {
    double[] probabilities = computeAnomalyProbabilities(
        temperature, depths, timestamp, latitude, longitude, woaGetter, carsGetter
    );
    
    return IntStream.range(0, depths.length)
        .filter(i -> probabilities[i] < threshold)
        .boxed()
        .collect(Collectors.toList());
  }
  
  protected static double[] computeAnomalyProbabilities(
      double[] temperature, double[] depths, long timestamp, double latitude, double longitude, WoaGetter woaGetter, CarsGetter carsGetter
  ) {
    double[] gradientProb = processGradient(temperature);
    double[] spikeProb = processSpike(temperature);
    double[] tukeyNormProb = processTukeyNorm(temperature);
    double[] rateOfChangeProb = processRateOfChange(temperature);
    double[] woaNormBiasProb = processWoaNormBias(
        temperature,
        depths,
        timestamp,
        latitude,
        longitude,
        woaGetter
    );
    double[] carsNormBiasProb = processCarsNormBias(
        temperature,
        depths,
        latitude,
        longitude,
        carsGetter
    );
    double[] constantClusterSizeProb = processConstantClusterSize(temperature);
    
    return IntStream.range(0, depths.length)
        .mapToDouble(i -> {
          double sum = 0;
          
          double gradient = gradientProb[i];
          if (!Double.isNaN(gradient)) {
            sum += gradient;
          }
          
          double spike = spikeProb[i];
          if (!Double.isNaN(spike)) {
            sum += spike;
          }
          
          double tukeyNorm = tukeyNormProb[i];
          if (!Double.isNaN(tukeyNorm)) {
            sum += tukeyNorm;
          }
          
          double rateOfChange = rateOfChangeProb[i];
          if (!Double.isNaN(rateOfChange)) {
            sum += rateOfChange;
          }
          
          double woaNormBias = woaNormBiasProb[i];
          if (!Double.isNaN(woaNormBias)) {
            sum += woaNormBias;
          }
          
          double carsNormBias = carsNormBiasProb[i];
          if (!Double.isNaN(carsNormBias)) {
            sum += carsNormBias;
          }
          
          double constantClusterSize = constantClusterSizeProb[i];
          if (!Double.isNaN(constantClusterSize)) {
            sum += constantClusterSize;
          }
          
          return sum;
        }).toArray();
  }
  
  protected static double[] processGradient(double[] input) {
    return computeWeibullSF(
        CoTeDeGradient.computeGradient(input),
        13.37195,
        0.245429,
        0.011027,
        0.000161
    );
  }
  
  protected static double[] processSpike(double[] input) {
    return computeWeibullSF(
        CoTeDeSpike.computeSpikes(input),
        2.724854,
    0.16998,
    0.001,
    4.4e-05
    );
  }
  
  protected static double[] processTukeyNorm(double[] input) {
    return computeWeibullSF(
      CoTeDeTukey53H.computeTukey53H(input, true), 
        4.497367,
        0.351177,
        0.001612,
        0.000236  
    );
  }
  
  protected static double[] processRateOfChange(double[] input) {
    return computeWeibullSF(
        CoTeDeRateOfChange.computeRateOfChange(input),
        1.478929,
        0.523945,
        0.036701,
        0.045529
    );
  }
  
  protected static double[] processWoaNormBias(
      double[] input, double[] depths, long timestamp, double latitude, double longitude, WoaGetter woaGetter
  ) {
    return computeWeibullSF(
        Arrays.stream(CoTeDeWoaNormbias.computeNormBiases(timestamp, longitude, latitude, depths, input, woaGetter))
            .map(Math::abs)
            .toArray(),
        5.960434,
        0.336008,
        1.705502,
        0.04268
    );
  }
  
  protected static double[] processCarsNormBias(double[] input, double[] depths, double latitude, double longitude, CarsGetter carsGetter) {
    return computeWeibullSF(
        Arrays.stream(CoTeDeCarsNormbias.computeCarsNormbiases(input, depths, latitude, longitude, carsGetter))
            .map(Math::abs).toArray(),
        0.96774,
        0.80382,
        1.80973,
        0.980883
    );
  }
  
  protected static double[] processConstantClusterSize(double[] input) {
    return computeWeibullSF(
        Arrays.stream(ConstantClusterSize.computeClusterSizes(input, 0))
            .mapToDouble(i -> (double) i)
            .toArray(),
        2.819822,
        0.234766,
        4.0,
        0.702378
    );
  }
  
  private static double[] computeWeibullSF(double[] input, double exponentiation, double shape, double location, double scale) {
    return Arrays.stream(input).map(value -> {
      if (Double.isNaN(value)) {
        return Double.NaN;
      }

      double val = 1 - Math.pow(-(Math.exp(-Math.pow((value - location) / scale, shape)) - 1), exponentiation);
      if (Double.isNaN(val)) {
        val = 1.0;
      } else if (val == 0.) {
        val = 1e-25;
      }

      return Math.log(val);
    }).toArray();
  }

}
