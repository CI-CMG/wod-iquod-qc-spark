package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.linear.MatrixUtils;

public class CoTeDeGradient {


  public static double[] computeGradient(double[] input) {
    int inputLength = input.length;
    double[] output = new double[inputLength];
    Arrays.fill(output, Double.NaN);

    if (inputLength == 1) {
      return output;
    }

    double[] outputVector = MatrixUtils.createRealVector(
        Arrays.copyOfRange(input, 1, inputLength - 1)
    ).subtract(
        MatrixUtils.createRealVector(
            Arrays.copyOfRange(input, 0, inputLength - 2)
        ).add(
            MatrixUtils.createRealVector(
                Arrays.copyOfRange(input, 2, inputLength)
            )
        ).mapDivide(2D)
    ).toArray();

    IntStream.range(0, outputVector.length).forEach(
        i -> output[i + 1] = outputVector[i]
    );

    return output;
  }

  public static Collection<Integer> getFlags(double[] input, double[] gradient, double threshold) {
    List<Integer> failedDepths = new ArrayList<>(0);
    for (int i = 0; i < input.length; i++) {
      if (Math.abs(gradient[i]) > threshold) {
        failedDepths.add(i);
      }
    }
    return failedDepths;
  }
}
