package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import edu.colorado.cires.wod.iquodqc.common.en.EnUtils;
import edu.colorado.cires.wod.iquodqc.common.en.GridCell;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

public class CastParameterDataReader {

  private final List<Double> clim;
  private final List<Double> bgev;

  public CastParameterDataReader(Cast cast, EnBgCheckInfoParameters parameters) {
    GridCell gridCell = EnUtils.findGridCell(cast, parameters);
    double[] clim = getArrayValues(cast.getMonth(), gridCell, parameters.getClim());
    double[] bgev = getArrayValues(gridCell, parameters.getBgev());

    List<Double> climList = new ArrayList<>(clim.length);
    List<Double> bgevList = new ArrayList<>(clim.length);

    for (int i = 0; i < clim.length; i++) {
      climList.add(Precision.equals(clim[i], parameters.getClimFillValue()) ? null : clim[i]);
      bgevList.add(Precision.equals(bgev[i], parameters.getBgevFillValue()) ? null : bgev[i]);
    }

    this.clim = Collections.unmodifiableList(climList);
    this.bgev = Collections.unmodifiableList(bgevList);
  }

  public List<Double> getClim() {
    return clim;
  }

  public List<Double> getBgev() {
    return bgev;
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
