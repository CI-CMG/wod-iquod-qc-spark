package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.linear.MatrixUtils;

public class CoTeDeSpike {

  public static List<Integer> checkSpike(double[] input, double threshold) {
    int inputLength = input.length;
    double[] output = new double[inputLength];
    Arrays.fill(output, Double.NaN);

    double[] vector1 = Arrays.stream(MatrixUtils.createRealVector(Arrays.copyOfRange(input, 1, inputLength - 1)).subtract(
        MatrixUtils.createRealVector(
            Arrays.copyOfRange(input, 0, inputLength - 2)
        ).add(
            MatrixUtils.createRealVector(
                Arrays.copyOfRange(input, 2, inputLength)
            )
        ).mapDivide(2D)
    ).toArray()).map(Math::abs).toArray();

    double[] vector2 = Arrays.stream(
        MatrixUtils.createRealVector(
            Arrays.copyOfRange(input, 2, inputLength)
        ).subtract(
            MatrixUtils.createRealVector(
                Arrays.copyOfRange(input, 0, inputLength - 2)
            )
        ).mapDivide(2D).toArray()
    ).map(Math::abs).toArray();

    double[] result = MatrixUtils.createRealVector(vector1).subtract(
        MatrixUtils.createRealVector(vector2)
    ).toArray();

    IntStream.range(0, result.length).forEach(i -> output[i + 1] = result[i]);
    return IntStream.range(0, input.length).boxed()
        .filter(i -> {
          boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
          if (i == 0) {
            return inputWasInvalid;
          }
          if (inputWasInvalid) {
            return true;
          }
          double value = output[i];
          if (Double.isNaN(value) || !Double.isFinite(value)) {
            return false;
          }
          return Math.abs(value) > threshold;
        }).collect(Collectors.toList());
  }

}
