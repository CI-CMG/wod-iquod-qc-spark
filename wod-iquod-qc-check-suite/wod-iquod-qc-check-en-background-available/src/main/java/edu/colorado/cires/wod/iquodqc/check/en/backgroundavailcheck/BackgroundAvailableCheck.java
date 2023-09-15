package edu.colorado.cires.wod.iquodqc.check.en.backgroundavailcheck;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ArrayUtils;
import edu.colorado.cires.wod.iquodqc.common.en.EnUtils;
import edu.colorado.cires.wod.iquodqc.common.en.GridCell;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.DoubleStream;
import org.apache.spark.sql.Row;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

public class BackgroundAvailableCheck extends CommonCastCheck {

  private final static String NAME = "EN_background_available_check";

  private static EnBgCheckInfoParameters parameters;
  private Properties properties;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (parameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    GridCell gridCell = EnUtils.findGridCell(cast, parameters);
    int month = cast.getMonth() - 1;
    double[] clim;
    try {
      clim = (double[]) parameters.getClim()
          .section(Arrays.asList(
              null,
              Range.make(gridCell.getiLat(), gridCell.getiLat()),
              Range.make(gridCell.getiLon(), gridCell.getiLon()),
              Range.make(month, month)
          )).get1DJavaArray(DataType.DOUBLE);
    } catch (InvalidRangeException e) {
      throw new RuntimeException(e);
    }

    boolean[] mask = ArrayUtils.getMask(clim, parameters.getFillValue());

    double[] ncDepths = (double[]) parameters.getDepth().get1DJavaArray(DataType.DOUBLE);

    clim = ArrayUtils.mask(clim, mask);
    if (clim.length == 0) {
      for (int i = 0; i < depths.size(); i++) {
        failures.add(i);
      }
      return failures;
    }

    ncDepths = ArrayUtils.mask(ncDepths, mask);

    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      Optional<ProfileData> maybeTemp = getTemperature(depth);

      if (maybeTemp.isPresent() && interpolate(depth.getDepth(), ncDepths, clim)) {
        failures.add(i);
      }

    }

    return failures;
  }

  private static void loadParameters(Properties properties) {
    synchronized (BackgroundAvailableCheck.class) {
      if (parameters == null) {
        parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
      }
    }
  }






  private static boolean interpolate(double z, double[] depths, double[] clim) {

//    LinearInterpolator interp = new LinearInterpolator();
//    PolynomialSplineFunction f = interp.interpolate(depths, clim);
//
//    if (f.isValidPoint(z)) {
//      return OptionalDouble.of(f.value(z));
//    }
//
//    return OptionalDouble.empty();

    /*
    Original Python logic was:

      # Get the climatology and error variance values at this level.
        climLevel = np.interp(z[iLevel], depths, clim, right=99999)
        if climLevel == 99999:
            qc[iLevel] = True # This could reject some good data if the
                              # climatology is incomplete, but also can act as
                              # a check that the depth of the profile is
                              # consistent with the depth of the ocean.

     This seems flawed though since a failure only happens when z > the maximum depth and the interpolated value is
     never used.

     numpy doc:

     right optional float or complex corresponding to fp
     Value to return for x > xp[-1], default is fp[-1].
     */

    return z > DoubleStream.of(depths).max().orElseThrow(() -> new IllegalStateException("depths was empty"));
  }

}
