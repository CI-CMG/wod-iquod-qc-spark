package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.math3.linear.MatrixUtils;

public class CoTeDeSpike {

  public static double[] computeSpikes(double[] input) {
    int inputLength = input.length;
    double[] output = new double[inputLength];
    Arrays.fill(output, Double.NaN);

    if (inputLength == 1) {
      return new double[]{Double.NaN};
    }

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

    return output;
  }

  public static Collection<Integer> getFlags(double[] input, double[] spikes, double threshold) {
    List<Integer> failedDepths = new ArrayList<>();
    for (int i = 1; i < input.length - 2; i++) {
      boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
      if (!inputWasInvalid && Math.abs(spikes[i]) > threshold) {
        failedDepths.add(i);
      }
    }
    return failedDepths;
  }
}
