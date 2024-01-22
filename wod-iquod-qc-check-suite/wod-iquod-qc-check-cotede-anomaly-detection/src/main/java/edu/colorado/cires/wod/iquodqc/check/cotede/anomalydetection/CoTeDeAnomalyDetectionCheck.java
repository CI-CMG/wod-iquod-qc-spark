package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;

public class CoTeDeAnomalyDetectionCheck extends CommonCastCheck {

  private static final double THRESHOLD = -18.0;

  private static WoaGetter woaGetter;
  private static CarsGetter carsGetter;
  private Properties properties;

  @Override
  public String getName() {
    return CheckNames.COTEDE_ANOMALY_DETECTION_CHECK.getName();
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    synchronized (CoTeDeAnomalyDetectionCheck.class) {
      if (woaGetter == null) {
        loadWoa(properties);
      }
      if (carsGetter == null) {
        loadCars(properties);
      }
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();

    double[] depthValues = new double[depths.size()];
    double[] temperatureValues = new double[depths.size()];

    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      depthValues[i] = depth.getDepth();
      temperatureValues[i] = getTemperature(depth).map(ProfileData::getValue)
          .orElse(Double.NaN);
    }

//    StringBuilder message = new StringBuilder("CoTeDeAnomalyDetection.checkFlags(");
//    message.append(" temperatureValues: ").append(Arrays.toString(temperatureValues));
//    message.append(" depthValues: ").append(Arrays.toString(depthValues));
//    message.append(" timestamp: ").append(cast.getTimestamp());
//    message.append(" latitude: ").append(cast.getLatitude());
//    message.append(" longitude: ").append(cast.getLongitude()).append(" )");
//
//    System.err.println(message);
//    System.out.println(message);

    return CoTeDeAnomalyDetection.checkFlags(
        temperatureValues,
        depthValues,
        cast.getTimestamp(),
        cast.getLatitude(),
        cast.getLongitude(),
        woaGetter,
        carsGetter,
        THRESHOLD
    );
  }

  private static void loadWoa(Properties properties) {
    WoaParameters woaParameters = WoaParametersReader.loadParameters(properties);
    woaGetter = new WoaGetter(woaParameters);
  }

  private static void loadCars(Properties properties) {
    CarsParameters carsParameters = CarsParametersReader.loadParameters(properties);
    carsGetter = new CarsGetter(carsParameters);
  }
}
