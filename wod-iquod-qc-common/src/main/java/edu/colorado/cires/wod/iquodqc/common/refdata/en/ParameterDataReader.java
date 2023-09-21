package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.DataType;

public class ParameterDataReader {

  private final List<Double> obev;
  private final List<Double> depths;

  public ParameterDataReader(EnBgCheckInfoParameters parameters) {
    double[] obev = (double[]) parameters.getObev().get1DJavaArray(DataType.DOUBLE);
    double[] depths = (double[]) parameters.getDepth().get1DJavaArray(DataType.DOUBLE);

    List<Double> obevList = new ArrayList<>(obev.length);
    List<Double> depthsList = new ArrayList<>(obev.length);

    for (int i = 0; i < obev.length; i++) {
      obevList.add(Precision.equals(obev[i], parameters.getObevFillValue()) ? null : obev[i]);
      depthsList.add(depths[i]);
    }

    this.obev = Collections.unmodifiableList(obevList);
    this.depths = Collections.unmodifiableList(depthsList);
  }

  public List<Double> getObev() {
    return obev;
  }

  public List<Double> getDepths() {
    return depths;
  }


}
