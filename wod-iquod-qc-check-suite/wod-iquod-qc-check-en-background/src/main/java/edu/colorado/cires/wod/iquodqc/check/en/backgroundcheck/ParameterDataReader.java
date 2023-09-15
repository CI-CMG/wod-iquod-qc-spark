package edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck;

import edu.colorado.cires.wod.iquodqc.common.en.EnUtils;
import edu.colorado.cires.wod.iquodqc.common.en.GridCell;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

public class ParameterDataReader {

  private final double[] clim;
  private final double[] bgev;
  private final double[] obev;
  private final double[] depths;

  public ParameterDataReader(Cast cast, EnBgCheckInfoParameters parameters) {
    // Remove missing data points
    GridCell gridCell = EnUtils.findGridCell(cast, parameters);
    double[] clim = getArrayValues(cast.getMonth(), gridCell, parameters.getClim());
    double[] bgev = getArrayValues(gridCell, parameters.getBgev());
    double[] obev = (double[]) parameters.getObev().get1DJavaArray(DataType.DOUBLE);
    double[] depths = (double[]) parameters.getDepth().get1DJavaArray(DataType.DOUBLE);
    double[] okClim = new double[clim.length];
    double[] okBgev = new double[bgev.length];
    double[] okObev = new double[obev.length];
    double[] okDepths = new double[depths.length];
    int okI = 0;
    for (int i = 0; i < clim.length; i++) {
      boolean iOk = !Precision.equals(clim[i], parameters.getFillValue()) && !Precision.equals(bgev[i], parameters.getFillValue());
      if (iOk) {
        okClim[okI] = clim[i];
        okBgev[okI] = bgev[i];
        okObev[okI] = obev[i];
        okDepths[okI] = depths[i];
        okI++;
      }
    }
    if (okI != clim.length) {
      this.clim = Arrays.copyOfRange(okClim, 0, okI);
      this.bgev = Arrays.copyOfRange(okBgev, 0, okI);
      this.obev = Arrays.copyOfRange(okObev, 0, okI);
      this.depths = Arrays.copyOfRange(okDepths, 0, okI);
    } else {
      this.clim = clim;
      this.bgev = bgev;
      this.obev = obev;
      this.depths = depths;
    }
  }

  public double[] getClim() {
    return clim;
  }

  public double[] getBgev() {
    return bgev;
  }

  public double[] getObev() {
    return obev;
  }

  public double[] getDepths() {
    return depths;
  }

  private static double[] getArrayValues(GridCell gridCell, Array array) {
    double[] values;
    try {
      values = (double[]) array
          .section(Arrays.asList(
              null,
              Range.make(gridCell.getiLat(), gridCell.getiLat()),
              Range.make(gridCell.getiLon(), gridCell.getiLon())
          )).get1DJavaArray(DataType.DOUBLE);
    } catch (InvalidRangeException e) {
      throw new RuntimeException(e);
    }
    return values;
  }

  private static double[] getArrayValues(int month, GridCell gridCell, Array array) {
    double[] values;
    try {
      values = (double[]) array
          .section(Arrays.asList(
              null,
              Range.make(gridCell.getiLat(), gridCell.getiLat()),
              Range.make(gridCell.getiLon(), gridCell.getiLon()),
              Range.make(month - 1, month - 1)
          )).get1DJavaArray(DataType.DOUBLE);
    } catch (InvalidRangeException e) {
      throw new RuntimeException(e);
    }
    return values;
  }
}
