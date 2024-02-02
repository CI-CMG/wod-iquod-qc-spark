package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import static edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy.BuddyCheckFunctions.buddyCovariance;
import static edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy.BuddyCheckFunctions.determinePge;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.apache.spark.sql.functions.udf;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CastUtils;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundChecker;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.CastParameterDataReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.ParameterDataReader;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnBkgBuddyCheck extends CommonCastCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnBkgBuddyCheck.class);
  private static final int MAX_DISTANCE_M = 400000;
  private static final CoordinateReferenceSystem EPSG_4326;
  private static EnBgCheckInfoParameters parameters;
  private static ParameterDataReader parameterData;
  private static EnBackgroundChecker enBackgroundChecker;
  private static StdLevelResolver stdLevelResolver;
  private static List<Double> slev;
  private static List<Double> obev;
  private static final String NAME = CheckNames.EN_STD_LEV_BKG_AND_BUDDY_CHECK.getName();
  private static final String DISTANCE = NAME + "_distance";
  private static final String GEOHASH = NAME + "_geohash";
  private static final String TEMP_T = NAME + "_t";


  private static final Collection<String> DEPENDS_ON;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:4326");
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
    List<String> dependsOn = new ArrayList<>(StdLevelResolver.OTHER_TESTS);
    dependsOn.add(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName());
    DEPENDS_ON = Collections.unmodifiableList(dependsOn);
  }


  private Properties properties;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected void registerUdf(CastCheckContext context) {
    super.registerUdf(context);
    SparkSession spark = context.getSparkSession();
    spark.udf().register(DISTANCE, udf((UDF4<Double, Double, Double, Double, Double>) this::getDistanceUdf, DataTypes.DoubleType));
    spark.udf().register(GEOHASH, udf((UDF2<Double, Double, List<String>>) this::getGeoHashesUdf, DataTypes.createArrayType(DataTypes.StringType)));
  }


  @Override
  public Collection<String> dependsOn() {
    return DEPENDS_ON;
  }

  @Override
  protected Dataset<Row> createQuery(CastCheckContext context) {
    Dataset<Row> joined = super.createQuery(context);
    joined.createOrReplaceTempView(TEMP_T);
    Dataset<Row> buddies = context.getSparkSession().sql(
        "select A.*, " +
            "(select first(struct(C.*)) from "
            + "  (select struct(B.*) as result, " + DISTANCE + "(A.cast.longitude, A.cast.latitude, B.cast.longitude, B.cast.latitude) as distance "
            + "      from " + TEMP_T + " B "
            + "      where array_contains(" + GEOHASH + "(A.cast.longitude, A.cast.latitude), B.cast.geohash) "
//            + "        and A.cast.year == B.cast.year "
            + "        and A.cast.month = B.cast.month "
            + "        and A.cast.cruiseNumber != B.cast.cruiseNumber "
            + "        and A.cast.castNumber != B.cast.castNumber "
            + "        order by distance asc"
            + "   ) C where C.distance <= " + MAX_DISTANCE_M + " "
            + " ) as buddy "
            + "from " + TEMP_T + " A");
    return buddies;
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  private double getDistanceUdf(double lon1, double lat1, double lon2, double lat2) {
    try {
      return JTS.orthodromicDistance(new Coordinate(lat1, lon1), new Coordinate(lat2, lon2), EPSG_4326);
    } catch (Exception e) {
      LOGGER.warn("{}: Unable to calculate distance: (" + lon1 + "," + lat1 + ") -> (" + lon2 + "," + lat2 + ") " + ExceptionUtils.getStackTrace(e), getName());
      return MAX_DISTANCE_M + 1;
    }
  }

  private List<String> getGeoHashesUdf(double lon, double lat) {
    try {
      return new ArrayList<>(GeoHashFinder.getNeighborsInDistance(lon, lat, MAX_DISTANCE_M));
    } catch (Exception e) {
      LOGGER.warn("{}: Unable to calculate geohash for " + lon + " lon, " + lat + " lat " + ExceptionUtils.getStackTrace(e), getName());
      return Collections.emptyList();
    }
  }

  @Override
  protected Row checkUdf(Row row) {
    if (parameters == null) {
      loadParameters(properties);
    }

    Row castRow = row.getStruct(row.fieldIndex("cast"));
    Map<String, CastCheckResult> otherTestResults = new HashMap<>();
    for (String otherTestName : dependsOn()) {
      CastCheckResult otherTestResult = CastCheckResult.builder(row.getStruct(row.fieldIndex(otherTestName))).build();
      otherTestResults.put(otherTestName, otherTestResult);
    }
    Cast buddy = null;
    double distance = -1D;
    Map<String, CastCheckResult> buddyOtherTestResults = new HashMap<>();
    Row buddyRow = row.getStruct(row.fieldIndex("buddy"));
    if (buddyRow != null) {
      Row resultRow = buddyRow.getStruct(buddyRow.fieldIndex("result"));
      distance = buddyRow.getDouble(buddyRow.fieldIndex("distance"));
      Row buddyCastRow = resultRow.getStruct(resultRow.fieldIndex("cast"));
      buddy = filterFlags(Cast.builder(buddyCastRow).build());
      for (String otherTestName : dependsOn()) {
        CastCheckResult otherTestResult = CastCheckResult.builder(resultRow.getStruct(resultRow.fieldIndex(otherTestName))).build();
        buddyOtherTestResults.put(otherTestName, otherTestResult);
      }
    }
    Cast cast = filterFlags(Cast.builder(castRow).build());
    Collection<Integer> failed = getFailedDepths(cast, otherTestResults, buddy, buddyOtherTestResults, distance);
    return CastCheckResult.builder()
        .withCastNumber(cast.getCastNumber())
        .withPassed(failed.isEmpty())
        .withFailedDepths(new ArrayList<>(failed))
        .build().asRow();
  }

  private static void loadParameters(Properties properties) {
    synchronized (EnBkgBuddyCheck.class) {
      if (parameters == null) {
        parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
        parameterData = new ParameterDataReader(parameters);
        slev = parameterData.getDepths();
        obev = parameterData.getObev();
        enBackgroundChecker = new EnBackgroundChecker(parameters);
        stdLevelResolver = new StdLevelResolver(enBackgroundChecker, slev);
      }
    }
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new UnsupportedOperationException("This method is not used");
  }

  private static boolean hasTemperature(Cast cast) {
    return cast.getDepths().stream().flatMap(d -> d.getData().stream().filter(pd -> pd.getVariableCode() == TEMPERATURE)).findAny().isPresent();
  }

  private List<StdLevel> getStdLevels(Cast cast, Map<String, CastCheckResult> otherTestResults, CastParameterDataReader castParameterData) {
    return getStdLevels(stdLevelResolver.getStdLevels(cast, otherTestResults), cast,  castParameterData);
  }

  private List<StdLevel> getStdLevels(List<StdLevel> stdLevels, Cast cast, CastParameterDataReader castParameterData) {
    List<Double> bgev = castParameterData.getBgev();
    Integer probeType = CastUtils.getProbeType(cast).map(Attribute::getValue).map(Double::intValue).orElse(null);
    for (StdLevel stdLevel : stdLevels) {
      if (stdLevel.getPge() == null) {
        stdLevel.setPge(determinePge(stdLevel.getStdLevelIndex(), stdLevel.getStdLevel(), bgev, obev, cast.getLatitude(), probeType));
      }
    }
    return stdLevels;
  }

  private Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults, @Nullable Cast buddy, Map<String, CastCheckResult> buddyOtherTestResults, double distance) {

//    try {
      Set<Integer> failures = new TreeSet<>();

      TreeMap<Integer, StdLevel> pgeLevels = new TreeMap<>();

      CastParameterDataReader parameterData = new CastParameterDataReader(cast, parameters);
      List<StdLevel> stdLevels = getStdLevels(cast, otherTestResults, parameterData);
      for (StdLevel stdLevel : stdLevels) {
        pgeLevels.put(stdLevel.getStdLevelIndex(), stdLevel);
      }

      if (!stdLevels.isEmpty()) {
        if (buddy != null) {
          // buddy vetos
          if (hasTemperature(buddy)) {
            CastParameterDataReader buddyParameterData = new CastParameterDataReader(buddy, parameters);
            // not sure why we are passing the levels from the main cast.  This is what the Python test did.
            List<StdLevel> buddyStdLevels = getStdLevels(stdLevels, buddy, buddyParameterData);
            for (StdLevel buddyStdLevel : buddyStdLevels) {
              double buddyPge = buddyStdLevel.getPge();
              StdLevel stdLevel = pgeLevels.get(buddyStdLevel.getStdLevelIndex());
              if (stdLevel != null) {
                stdLevel.setPge(updatePge(
                    stdLevel.getPge(),
                    buddyPge,
                    cast,
                    buddy,
                    stdLevel.getStdLevel(),
                    buddyStdLevel.getStdLevel(),
                    distance,
                    parameterData.getBgev().get(stdLevel.getStdLevelIndex()),
                    buddyParameterData.getBgev().get(stdLevel.getStdLevelIndex()),
                    obev.get(stdLevel.getStdLevelIndex())));
              }
            }
          }
        }

        reinstateLevels(cast, pgeLevels, parameterData.getClim(), slev);

        pgeLevels.values().forEach(stdLevel -> {
          if (stdLevel.getPge() >= 0.5) {
            stdLevel.getLevelWrappers().forEach(lw -> {
              failures.add(lw.getLevel().getOrigLevel());
            });
          }
        });
      }

      return failures;
//    } catch (RuntimeException e) {
//      StringBuilder stringBuilder = new StringBuilder();
//      stringBuilder.append("cast: ").append(cast);
//      stringBuilder.append("\notherTestResults: ").append(otherTestResults);
//      stringBuilder.append("\nbuddy: ").append(buddy);
//      stringBuilder.append("\nbuddyOtherTestResults: ").append(buddyOtherTestResults);
//      stringBuilder.append("\ndistance: ").append(distance);
//      System.out.println(stringBuilder);
//      throw e;
//    }

  }

  protected void reinstateLevels(Cast cast, TreeMap<Integer, StdLevel> pgeLevels, List<Double> bgsl, List<Double> slev) {
    final double depthTol = Math.abs(cast.getLatitude()) < 20D ? 300D : 200D;
    int nsl = new ArrayList<>(pgeLevels.keySet()).get(pgeLevels.size() - 1);
    for (StdLevel stdLevel : pgeLevels.values()) {
      if (stdLevel.getPge() >= 0.5) {
        int i = stdLevel.getStdLevelIndex();
        boolean okBelow = false;
        if (i > 0) {
          if(pgeLevels.get(i - 1) != null && bgsl.get(i - 1) != null && pgeLevels.get(i - 1).getPge() < 0.5){
            okBelow = true;
          }
        }
        boolean okAbove = false;
        if (i < nsl - 1) {
          if(pgeLevels.get(i + 1) != null && bgsl.get(i + 1) != null && pgeLevels.get(i + 1).getPge() < 0.5){
            okAbove = true;
          }
        }
        double depth = slev.get(i);
        double tolFactor;
        if (depth > depthTol + 100D) {
          tolFactor = 0.5;
        } else if (depth > depthTol) {
          tolFactor = 1.0 - 0.005 * (depth - depthTol);
        } else {
          tolFactor = 1D;
        }
        double ttol = 0.5 * tolFactor;
        double xMax;
        double xMin;
        if (okBelow && okAbove) {
          xMax = pgeLevels.get(i - 1).getStdLevel() + bgsl.get(i - 1) + ttol;
          xMin = pgeLevels.get(i + 1).getStdLevel() + bgsl.get(i + 1) - ttol;
        } else if (okBelow) {
          xMax = pgeLevels.get(i - 1).getStdLevel() + bgsl.get(i - 1) + ttol;
          xMin = pgeLevels.get(i - 1).getStdLevel() + bgsl.get(i - 1) - ttol;
        } else if (okAbove) {
          xMax = pgeLevels.get(i + 1).getStdLevel() + bgsl.get(i + 1) + ttol;
          xMin = pgeLevels.get(i + 1).getStdLevel() + bgsl.get(i + 1) - ttol;
        } else {
          continue;
        }
        if (pgeLevels.get(i).getStdLevel() + bgsl.get(i) >= xMin && pgeLevels.get(i).getStdLevel() + bgsl.get(i) <= xMax) {
          pgeLevels.get(i).setPge(0.49);
        }
      }
    }
  }

  private static double updatePge(double pge, double buddyPge, Cast cast, Cast buddy, double level, double levelBuddy, double minDist, double bgev, double bgevBuddy, double obev) {
    final double covar = buddyCovariance(minDist, cast, buddy, bgev / 2D, bgevBuddy / 2.0D, bgev / 2D, bgevBuddy / 2D);
    final double errVarA = obev + bgev;
    final double errVarB = obev + bgevBuddy;
    final double rho2 = Math.pow(covar, 2D) / (errVarA * errVarB);
    double expArg =
        -(0.5 * rho2 / (1.0 - rho2)) * (Math.pow(level, 2D) / errVarA + Math.pow(levelBuddy, 2D) / errVarB - 2D * level * levelBuddy / covar);
    expArg = -0.5 * Math.log(1D - rho2) + expArg;
    expArg = Math.min(80D, Math.max(-80D, expArg));
    double z = 1D / (1D - (1D - pge) * (1D - buddyPge) * (1D - expArg));
    //  In case of rounding errors.
    if (z < 0D) {
      z = 1D;
    }
    z = Math.pow(z, 0.5);
    return pge * z;
  }


}
