package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

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
    return IntStream.range(0, input.length)
        .filter(i -> {
          boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
          if (i == 0 || i == input.length - 1) {
            return inputWasInvalid;
          }
          if (inputWasInvalid) {
            return true;
          }
          double gradientValue = gradient[i];
          if (Double.isNaN(gradientValue) || !Double.isFinite(gradientValue)) {
            return false; // could not be evaluated because of nearby invalid value
          }
          return Math.abs(gradientValue) > threshold;
        }).boxed().collect(Collectors.toList());
  }
}
