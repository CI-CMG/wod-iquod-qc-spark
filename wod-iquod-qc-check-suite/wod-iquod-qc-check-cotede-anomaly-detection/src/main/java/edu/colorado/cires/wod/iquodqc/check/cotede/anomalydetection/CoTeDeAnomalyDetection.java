package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import java.util.Arrays;
import java.util.stream.IntStream;

public class CoTeDeAnomalyDetection {

  protected static double[] processGradient(double[] input) {
    return computeWeibullSF(
        input,
        13.37195,
        0.245429,
        0.011027,
        0.000161
    );
  }

  protected static double[] processSpike(double[] input) {
    return computeWeibullSF(
        input,
        2.724854,
        0.16998,
        0.001,
        4.4e-05
    );
  }

  protected static double[] processTukeyNorm(double[] input) {
    return computeWeibullSF(
        input,
        4.497367,
        0.351177,
        0.001612,
        0.000236
    );
  }

  protected static double[] processRateOfChange(double[] input) {
    return computeWeibullSF(
        input,
        1.478929,
        0.523945,
        0.036701,
        0.045529
    );
  }

  protected static double[] processWoaNormBias(double[] input) {
    return computeWeibullSF(
        Arrays.stream(input)
//            .map(Math::abs) // Another change made for AutoQC consistency. This should be converted to an absolute value here but AutoQC does not
            .toArray(),
        5.960434,
        0.336008,
        1.705502,
        0.04268
    );
  }

  protected static double[] processCarsNormBias(double[] input) {
    return computeWeibullSF(
        Arrays.stream(input)
            .map(Math::abs).toArray(),
        0.96774,
        0.80382,
        1.80973,
        0.980883
    );
  }

  protected static double[] processConstantClusterSize(double[] input) {
    return computeWeibullSF(
        input,
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

  protected static double[] computeAnomaly(
      double[] gradient,
      double[] spikes,
      double[] tukeyNorm,
      double[] rateOfChange,
      double[] woaNormbias,
      double[] carsNormbias,
      double[] constantClusterSize
  ) {
    double[] gradientProb = processGradient(gradient);
    double[] spikeProb = processSpike(spikes);
    double[] tukeyNormProb = processTukeyNorm(tukeyNorm);
    double[] rateOfChangeProb = processRateOfChange(rateOfChange);
    double[] woaNormBiasProb = processWoaNormBias(woaNormbias);
    double[] carsNormBiasProb = processCarsNormBias(carsNormbias);
    double[] constantClusterSizeProb = processConstantClusterSize(constantClusterSize);

    return IntStream.range(0, gradient.length)
        .mapToDouble(i -> {
          double sum = 0;

          double grad = gradientProb[i];
          if (!Double.isNaN(grad)) {
            sum += grad;
          }

          double spike = spikeProb[i];
          if (!Double.isNaN(spike)) {
            sum += spike;
          }

          double tukeyN = tukeyNormProb[i];
          if (!Double.isNaN(tukeyN)) {
            sum += tukeyN;
          }

          double roc = rateOfChangeProb[i];
          if (!Double.isNaN(roc)) {
            sum += roc;
          }

          double woaNormBias = woaNormBiasProb[i];
          if (!Double.isNaN(woaNormBias)) {
            sum += woaNormBias;
          }

          double carsNormBias = carsNormBiasProb[i];
          if (!Double.isNaN(carsNormBias)) {
            sum += carsNormBias;
          }

          double ccs = constantClusterSizeProb[i];
          if (!Double.isNaN(ccs)) {
            sum += ccs;
          }

          return sum;
        }).toArray();
  }

  public static int[] getFlags(
      double[] gradient,
      double[] spikes,
      double[] tukeyNorm,
      double[] rateOfChange,
      double[] woaNormbias,
      double[] carsNormbias,
      double[] constantClusterSize,
      double threshold
  ) {
    double[] probabilities = computeAnomaly(
        gradient,
        spikes,
        tukeyNorm,
        rateOfChange,
        woaNormbias,
        carsNormbias,
        constantClusterSize
    );

    return IntStream.range(0, probabilities.length)
        .filter(i -> probabilities[i] < threshold)
        .toArray();
  }

}
