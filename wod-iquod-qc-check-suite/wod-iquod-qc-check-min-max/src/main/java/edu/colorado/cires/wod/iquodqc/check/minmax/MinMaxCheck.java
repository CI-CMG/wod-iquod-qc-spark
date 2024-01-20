package edu.colorado.cires.wod.iquodqc.check.minmax;

import static edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader.WOD_INFO_DGG4H6_PROP;
import static edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader.WOD_TEMP_MIN_MAX_PROP;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getPressure;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import ucar.ma2.DataType;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;
import us.hebi.matlab.mat.format.Mat5;
import us.hebi.matlab.mat.format.Mat5File;
import us.hebi.matlab.mat.types.Source;
import us.hebi.matlab.mat.types.Sources;

public class MinMaxCheck extends CommonCastCheck {

  private Properties properties;
  private boolean downloadedResources;

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (!downloadedResources) {
      MinMaxParametersReader.downloadTempMinMax(properties);
      MinMaxParametersReader.downloadInfoDGG4H6(properties);
      
      downloadedResources = true;
    }
    return super.checkUdf(row);
  }

  @Override
  public String getName() {
    return CheckNames.MIN_MAX_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    try (
        Source source = Sources.openFile(properties.get("data.dir") + File.separator + WOD_INFO_DGG4H6_PROP + ".dat");
        Mat5File matLabFile = Mat5.newReader(source).readMat();
        NetcdfFile netcdfFile = NetcdfFiles.open(properties.get("data.dir") + File.separator + WOD_TEMP_MIN_MAX_PROP + ".dat")
    ) {
      int gridIndex = MinMax.getGridIndex(cast.getLatitude(), cast.getLongitude(), matLabFile);
      double[] depths = (double[]) Objects.requireNonNull(netcdfFile.findVariable("depth")).read().get1DJavaArray(DataType.DOUBLE);
      Variable minTemp = netcdfFile.findVariable("temp_min");
      Variable maxTemp = netcdfFile.findVariable("temp_max");

      List<Depth> castDepths = cast.getDepths();
      return IntStream.range(0, castDepths.size()).boxed()
          .filter(i -> {
            Depth depth = castDepths.get(i);
            double t = getTemperature(depth).map(ProfileData::getValue)
                .orElse(Double.NaN);
            double p = getPressure(depth).map(ProfileData::getValue)
                .orElse(Double.NaN);
            return !MinMax.checkMinMax(
                t,
                MinMax.getMinMax(p, depths, gridIndex, minTemp, maxTemp)
            );
          })
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
