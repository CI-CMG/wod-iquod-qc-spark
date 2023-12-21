package edu.colorado.cires.wod.iquodqc.check.cotede.locationatsea;


import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.EtopoParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.LocationInterpolationUtils;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.NetCdfEtopoParameters;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Row;

public class CoTeDeLocationAtSeaCheck extends CommonCastCheck {

  private static NetCdfEtopoParameters parameters;
  private Properties properties;

  @Override
  public String getName() {
    return CheckNames.COTEDE_LOCATION_AT_SEA_TEST.getName();
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
    double depth = LocationInterpolationUtils.depthInterpolation(parameters, cast.getLongitude(), cast.getLatitude());
    if (depth >= 0D) {
      return failed;
    } else {
      for (int i = 0; i < cast.getDepths().size(); i++) {
        failed.add(i);
      }
    }
    return failed;
  }

  private static void loadParameters(Properties properties) {
    synchronized (CoTeDeLocationAtSeaCheck.class) {
      if (parameters == null) {
        parameters = EtopoParametersReader.loadParameters(properties);
      }
    }
  }

}
