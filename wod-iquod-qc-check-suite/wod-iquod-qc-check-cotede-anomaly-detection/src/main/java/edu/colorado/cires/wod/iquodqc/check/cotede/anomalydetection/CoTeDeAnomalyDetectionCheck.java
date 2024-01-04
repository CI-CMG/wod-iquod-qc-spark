package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;

public class CoTeDeAnomalyDetectionCheck extends CommonCastCheck {
  
  private static final double THRESHOLD = -18.0;
  
  private static WoaGetter woaGetter;
  private static CarsGetter carsGetter;
  private static Properties properties;

  @Override
  public String getName() {
    return "COTEDE_ANOMALY_DETECTION_CHECK";
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (woaGetter == null) {
      loadWoa();
    }
    if (carsGetter == null) {
      loadCars();
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
  
  protected static void loadWoa() {
    WoaParameters woaParameters = WoaParametersReader.loadParameters(properties);
    woaGetter = new WoaGetter(woaParameters);
  }
  
  protected static void loadCars() {
    CarsParameters carsParameters = CarsParametersReader.loadParameters(properties);
    carsGetter = new CarsGetter(carsParameters);
  }
}
