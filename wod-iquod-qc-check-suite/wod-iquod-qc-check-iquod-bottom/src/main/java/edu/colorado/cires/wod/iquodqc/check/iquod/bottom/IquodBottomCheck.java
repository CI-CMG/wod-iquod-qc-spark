package edu.colorado.cires.wod.iquodqc.check.iquod.bottom;


import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.EtopoParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.LocationInterpolationUtils;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.NetCdfEtopoParameters;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Row;

public class IquodBottomCheck extends CommonCastCheck {

  private static NetCdfEtopoParameters parameters;
  private Properties properties;

  @Override
  public String getName() {
    return CheckNames.IQUOD_BOTTOM.getName();
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
    Set<Integer> failed = new LinkedHashSet<>();
    double floor = LocationInterpolationUtils.depthInterpolation(parameters, cast.getLongitude(), cast.getLatitude());
    List<Depth> depths = cast.getDepths();
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      if (depth.getDepth() > floor) {
        failed.add(i);
      }
    }
    return failed;
  }

  private static void loadParameters(Properties properties) {
    synchronized (IquodBottomCheck.class) {
      if (parameters == null) {
        parameters = EtopoParametersReader.loadParameters(properties);
      }
    }
  }

}
