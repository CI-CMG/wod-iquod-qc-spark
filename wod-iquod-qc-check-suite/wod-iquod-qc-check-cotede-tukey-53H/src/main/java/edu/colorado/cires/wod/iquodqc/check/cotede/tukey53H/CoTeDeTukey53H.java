package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.ranking.NaNStrategy;

public class CoTeDeTukey53H {
  
  public static List<Integer> checkTukey53H(double[] input, double threshold, boolean isNorm) {
    return getFlags(input, computeTukey53H(input, isNorm), threshold);
  }
  
  public static List<Integer> getFlags(double[] input, double[] result, double threshold) {
    return IntStream.range(0, result.length).boxed()
        .filter(i -> {
          double value = result[i];
          boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
          if (i < 2 || i >= input.length - 2) {
            return inputWasInvalid;
          }
          return Math.abs(value) > threshold || inputWasInvalid;
        }).collect(Collectors.toList());
  }

  public static double[] computeTukey53H(double[] input, boolean isNorm) {
    double[] median5 = computeWindowedMedian(input, 5);
    double[] median53H = computeWindowedMedian(median5, 3);

    double[] median53HLeft = Arrays.copyOf(median53H, median53H.length);
    shiftArray(median53HLeft, -1);
    double[] median53HRight = Arrays.copyOf(median53H, median53H.length);
    shiftArray(median53HRight, 1);

    RealVector vector1 = MatrixUtils.createRealVector(median53HLeft);
    RealVector vector2 = MatrixUtils.createRealVector(median53H).mapMultiply(2);
    RealVector vector3 = MatrixUtils.createRealVector(median53HRight);

    double[] tukey53H = MatrixUtils.createRealVector(input).subtract(
        vector1.add(vector2).add(vector3).mapMultiply(0.25)
    ).toArray();
    
    if (!isNorm) {
      return tukey53H;
    }
    
    return MatrixUtils.createRealVector(tukey53H)
        .mapDivide(
            new StandardDeviation().evaluate(
                Arrays.stream(median5)
                    .filter(v -> !Double.isNaN(v))
                    .toArray()
            )
        ).toArray();
  }

  private static void shiftArray(double[] input, int shift) {
    int length = input.length;
    shift = shift % length;

    double[] tempArray = Arrays.copyOf(input, length);

    for (int i = 0; i < length; i++) {
      int newIndex = (i + shift + length) % length;
      input[newIndex] = tempArray[i];
    }
  }

  private static double[] computeWindowedMedian(double[] input, int windowSize) {
    int inputLength = input.length;
    double[] output = new double[inputLength];

    if (input.length == 1) {
      output[0] = input[0];
      return output;
    }

    int halfWindowSizeFloor = (int) Math.floor((double) windowSize / 2);

    List<Double> queue = new ArrayList<>();
    for (int i = 0; i < halfWindowSizeFloor; i++) {
      queue.add(input[i]);
      output[i] = Double.NaN;

    }

    Median median = new Median().withNaNStrategy(NaNStrategy.FAILED);
    for (int i = halfWindowSizeFloor; i < inputLength; i++) {
      queue.add(input[i]);
      if (queue.size() == windowSize) {
        if (queue.contains(Double.NaN)) {
          output[i - halfWindowSizeFloor  ] = Double.NaN;
        } else {
          output[i - halfWindowSizeFloor  ] = median.evaluate(
              queue.stream()
                  .mapToDouble(Double::doubleValue)
                  .sorted()
                  .toArray()
          );
        }
        queue.remove(queue.get(0));
      }
    }

    for (int i = inputLength - halfWindowSizeFloor; i < inputLength; i++) {
      output[i] = Double.NaN;
    }

    return output;
  }

}
