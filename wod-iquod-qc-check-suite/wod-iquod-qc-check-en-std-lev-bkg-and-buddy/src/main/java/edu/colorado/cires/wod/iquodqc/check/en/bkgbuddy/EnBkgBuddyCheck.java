package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

import com.github.davidmoten.geo.GeoHash;
import com.google.common.annotations.VisibleForTesting;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CastUtils;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundChecker;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerLevelResult;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerResult;
import edu.colorado.cires.wod.iquodqc.common.en.PgeEstimator;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.ParameterDataReader;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

public class EnBkgBuddyCheck extends CommonCastCheck {


  private static EnBgCheckInfoParameters parameters;
  private Properties properties;

  private static final Set<String> OTHER_TEST_TO_CHECK = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      CheckNames.EN_BACKGROUND_CHECK.getName(),
      CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
      CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
      CheckNames.EN_RANGE_CHECK.getName(),
      CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
      CheckNames.EN_STABILITY_CHECK.getName()
  )));

  @Override
  public String getName() {
    return CheckNames.EN_STD_LEV_BKG_AND_BUDDY_CHECK.getName();
  }

  @Override
  protected void registerUdf(CastCheckContext context) {
    super.registerUdf(context);
    SparkSession spark = context.getSparkSession();
    spark.udf().register(getName() + "_distance", udf((UDF4<Double, Double, Double, Double, Double>) this::getDistanceUdf, DataTypes.DoubleType));
    spark.udf().register(getName() + "_geohash", udf((UDF2<Double, Double, List<String>>) this::getGeoHashesUdf, DataTypes.createArrayType(DataTypes.StringType)));
  }

  @Override
  public Collection<String> dependsOn() {
    return Arrays.asList(
        CheckNames.EN_BACKGROUND_CHECK.getName(),
        CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
        CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
        CheckNames.EN_RANGE_CHECK.getName(),
        CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
        CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName(), // needed for EnBackgroundChecker
        CheckNames.EN_STABILITY_CHECK.getName()
    );
  }


  @Override
  protected Dataset<Row> createQuery(CastCheckContext context) {
    Dataset<Row> joined = super.createQuery(context);
    joined.createOrReplaceTempView(getName() + "_t");
    Dataset<Row> buddies = context.getSparkSession().sql(
        "select A.*, " +
            "(select first(C.result) from (select struct(B.*) as result, " + getName() + "_distance(A.cast.longitude, A.cast.latitude, B.cast.longitude, B.cast.latitude) as distance from " + getName() + "_t B where array_contains(" + getName() + "_geohash(A.cast.longitude, A.cast.latitude), B.cast.geohash) and A.cast.year == B.cast.year and A.cast.month = B.cast.month and A.cast.cruiseNumber != B.cast.cruiseNumber and A.cast.castNumber != B.cast.castNumber order by distance asc) C ) as buddy "+
            "from " + getName() + "_t  A");
    return buddies;
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  private static final CoordinateReferenceSystem EPSG_4326;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:4326");
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
  }

  private double getDistanceUdf(double lon1, double lat1, double lon2, double lat2) {
    try {
      return JTS.orthodromicDistance(new Coordinate(lon1, lat1), new Coordinate(lon2, lat2), EPSG_4326);
    } catch (TransformException e) {
      throw new RuntimeException("Unable to calculate distance: (" + lon1 + "," + lat1 + ") -> (" + lon2 + "," + lat2 + ")",  e);
    }
  }

  private static final int MAX_DISTANCE_M = 400000;

  private List<String> getGeoHashesUdf(double lon, double lat) {
    return new ArrayList<>(GeoHashFinder.getNeighborsInDistance(lon, lat, MAX_DISTANCE_M));
  }

  @Override
  protected Row checkUdf(Row row) {
    if (parameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  private static void loadParameters(Properties properties) {
    synchronized (EnBkgBuddyCheck.class) {
      if (parameters == null) {
        parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
      }
    }
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new UnsupportedOperationException("This method is not used");
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {


    ParameterDataReader parameterData = new ParameterDataReader(cast, parameters);
    double[] bgsl = parameterData.getClim();
    double[] slev = parameterData.getDepths();
    double[] bgev = parameterData.getBgev();
    double[] obev = parameterData.getObev();

    int probeType = 1; // TODO
    boolean failedSpikeTest = false; // TODO

    List<StdLevel> stdLevels = getStdLevels(cast, otherTestResults, slev);
    for (StdLevel stdLevel: stdLevels) {
      double pge = determinePge(stdLevel.getStdLevelIndex(), stdLevel.getMeanDifference(), bgev, obev, cast.getLatitude(), probeType, failedSpikeTest);

    }


//
//    List<Depth> levels = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();
//
//    EnBackgroundCheckerResult bgResult = EnBackgroundChecker.getFailedDepths(cast, otherTestResults, parameters);
//    for (EnBackgroundCheckerLevelResult levelResult : bgResult.getLevels()) {
//      if (!preQc.contains(levelResult.getOrigLevel())) {
//        StdLevelWrapper stdLevelWrapper = atStandardLevel(levels, levelResult, slev);
//        double pge = determinePge(stdLevelWrapper.getStdLevelIndex(), stdLevelWrapper.getAvgLevel(), bgev, obev, cast.getLatitude(), probeType, failedSpikeTest);
//      }
//
//    }
//
//    for (int i = 0; i < levels.size(); i++) {
//      Depth levelDepth = levels.get(i);
//
//    }

    return failures;
  }

//  private Map<String, CastCheckResult> getTestResultsForBuddy(Cast buddy) {
//
//  }
//
//  private Optional<Cast> findBuddy(Cast cast, double maxDistance, EnBgCheckInfoParameters parameters) {
//
//  }

  /*
  def record_parameters(profile, bgStdLevels, bgevStdLevels, origLevels, ptLevels, bgLevels, data_store):
    # pack the parameter arrays into the enbackground table
    # for consumption by the buddy check
    data_store.put(profile.uid(), 'enbackground', {'bgstdlevels':bgStdLevels, 'bgevstdlevels':bgevStdLevels, 'origlevels':origLevels, 'ptlevels':ptLevels, 'bglevels':bgLevels})

   */


//  private static void stdLevelData(Cast cast, Map<String, CastCheckResult> otherTestResults, ParameterDataReader parameterData) {
//    Set<Integer> preQc = new HashSet<>();
//    otherTestResults.values().stream().map(CastCheckResult::getFailedDepths).forEach(preQc::addAll);
//
//    EnBackgroundCheckerResult bgResult = EnBackgroundChecker.getFailedDepths(cast, otherTestResults, parameters);
//
//    double[] stdLevels = parameterData.getDepths();
//
//    boolean failedSpikeTest = otherTestResults.get(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName()).getFailedDepths().contains(iLevel);
//
//    CastUtils.getProbeType(cast).map(Attribute::getValue).map(Double::intValue).ifPresent(probeType -> {
//    });
//
//    bgResult.getLevels().stream().filter(level -> !preQc.contains(level.getOrigLevel())).forEach(levelResult -> {
//
//    });
//
//
//  }

  private static double determinePge(int iLevel, double level, double[] bgev, double[] obev, double latitude, int probeType, boolean failedSpikeTest) {
    double bgevLevel = bgev[iLevel];
    if (Math.abs(latitude) < 10D) {
      bgevLevel *= Math.pow(1.5, 2D);
    }
    double obevLevel = obev[iLevel];
    double pgeEst = PgeEstimator.estimateProbabilityOfGrossError(probeType, failedSpikeTest);
    double kappa = 0.1;
    double evLevel = obevLevel + bgevLevel; // V from the text
    double sdiff = Math.pow(level, 2D) / evLevel;
    double pdGood = Math.exp(-0.5 * Math.min(sdiff, 160D)) / Math.sqrt(2D * Math.PI * evLevel);
    double pdTotal = kappa * pgeEst + pdGood * (1D - pgeEst);
    return kappa * pgeEst / pdTotal;
  }

//  private static List<Double> determinePge(List<Double> levels, double[] bgev, double[] obev, double latitude, int probeType, boolean failedSpikeTest) {
//    /*
//    determine the probability of gross error per level given:
//    levels: a list of observed - background temperatures per level (ie the first return of stdLevelData)
//    bgev: list of background error variance per level
//    obev: list of observational error variances per level
//    profile: the wodpy profile object in question
//     */
//    List<Double> pge = new ArrayList<>(levels.size());
//    for (int iLevel = 0; iLevel < levels.size(); iLevel++) {
//      Double level = levels.get(iLevel);
//      if (level != null) {
//        double bgevLevel = bgev[iLevel];
//        if (Math.abs(latitude) < 10D){
//          bgevLevel *= Math.pow(1.5, 2D);
//        }
//        double obevLevel = obev[iLevel];
//        double pgeEst = PgeEstimator.estimateProbabilityOfGrossError(probeType, failedSpikeTest);
//        double kappa = 0.1;
//        double evLevel = obevLevel + bgevLevel; // V from the text
//        double sdiff   = Math.pow(level, 2D) / evLevel;
//        double pdGood = Math.exp(-0.5 * Math.min(sdiff, 160D)) / Math.sqrt(2D * Math.PI * evLevel);
//        double pdTotal = kappa * pgeEst + pdGood * (1D - pgeEst);
//        pge.add(kappa * pgeEst / pdTotal);
//      } else {
//        pge.add(null);
//      }
//    }
//    return pge;
//  }

  /*
  def determine_pge(levels, bgev, obev, profile):
    '''
    determine the probability of gross error per level given:
    levels: a list of observed - background temperatures per level (ie the first return of stdLevelData)
    bgev: list of background error variance per level
    obev: list of observational error variances per level
    profile: the wodpy profile object in question
    '''
    pge = np.ma.array(np.ndarray(len(levels)))
    pge.mask = True

    for iLevel, level in enumerate(levels):
        if levels.mask[iLevel] or bgev.mask[iLevel]: continue
        bgevLevel = bgev[iLevel]
        if np.abs(profile.latitude()) < 10.0: bgevLevel *= 1.5**2
        obevLevel = obev[iLevel]
        pge_est = EN_background_check.estimatePGE(profile.probe_type(), False)

        kappa   = 0.1
        evLevel = obevLevel + bgevLevel  #V from the text
        sdiff   = level**2 / evLevel
        pdGood  = np.exp(-0.5 * np.min([sdiff, 160.0])) / np.sqrt(2.0 * np.pi * evLevel)
        pdTotal = kappa * pge_est + pdGood * (1.0 - pge_est)
        pge[iLevel] = kappa * pge_est / pdTotal

    return pge
   */

  private static class StdLevelWrapper {

    private final EnBackgroundCheckerLevelResult level;
    private final int stdLevelIndex;
    private final double stdLevelDifference;
    private final double diffLevel;

    private StdLevelWrapper(EnBackgroundCheckerLevelResult level, int stdLevelIndex, double stdLevelDifference, double diffLevel) {
      this.level = level;
      this.stdLevelIndex = stdLevelIndex;
      this.stdLevelDifference = stdLevelDifference;
      this.diffLevel = diffLevel;
    }

    public EnBackgroundCheckerLevelResult getLevel() {
      return level;
    }

    public int getStdLevelIndex() {
      return stdLevelIndex;
    }

    public double getStdLevelDifference() {
      return stdLevelDifference;
    }

    public double getDiffLevel() {
      return diffLevel;
    }
  }

  private static StdLevelWrapper atStandardLevel(List<Depth> depths, EnBackgroundCheckerLevelResult level, double[] slev) {
    double diffLevel = level.getPtLevel() - level.getBgLevel();
    // Find the closest standard level
    int minIndex = -1;
    double minDiff = Integer.MAX_VALUE;
    for (int j = 0; j < slev.length; j++) {
      double diff = Math.abs(depths.get(level.getOrigLevel()).getDepth() - slev[j]);
      if (minIndex < 0 || diff < minDiff) {
        minIndex = j;
        minDiff = diff;
      }
    }
    return new StdLevelWrapper(level, minIndex, minDiff, diffLevel);
  }

  private static class StdLevel {
    private final int stdLevelIndex;
    private final double stdLevel;
    private final List<StdLevelWrapper> levelWrappers = new ArrayList<>();

    public StdLevel(int stdLevelIndex, double stdLevel) {
      this.stdLevelIndex = stdLevelIndex;
      this.stdLevel = stdLevel;
    }

    public int getStdLevelIndex() {
      return stdLevelIndex;
    }

    public double getStdLevel() {
      return stdLevel;
    }

    public List<StdLevelWrapper> getLevelWrappers() {
      return levelWrappers;
    }

    public double getMeanDifference() {
      return levelWrappers.stream().mapToDouble(StdLevelWrapper::getStdLevelDifference).reduce(0, Double::sum) / (double) levelWrappers.size();
    }
  }

  private static List<StdLevel> getStdLevels(Cast cast, Map<String, CastCheckResult> otherTestResults, double[] slev) {
    final Set<Integer> preQc = new HashSet<>();
    for (Entry<String, CastCheckResult> entry : otherTestResults.entrySet()) {
      if (OTHER_TEST_TO_CHECK.contains(entry.getKey())) {
        preQc.addAll(entry.getValue().getFailedDepths());
      }
    }

    Map<Integer, StdLevel> stdLevelMap = new TreeMap<>();

    EnBackgroundCheckerResult bgResult = EnBackgroundChecker.getFailedDepths(cast, otherTestResults, parameters);
    for (EnBackgroundCheckerLevelResult levelResult : bgResult.getLevels()) {
      if (!preQc.contains(levelResult.getOrigLevel())) {
        StdLevelWrapper stdLevelWrapper = atStandardLevel(cast.getDepths(), levelResult, slev);
        StdLevel stdLevel = stdLevelMap.get(stdLevelWrapper.getStdLevelIndex());
        if (stdLevel == null) {
          stdLevel = new StdLevel(stdLevelWrapper.getStdLevelIndex(), slev[stdLevelWrapper.getStdLevelIndex()]);
          stdLevelMap.put(stdLevelWrapper.getStdLevelIndex(), stdLevel);
        }
        stdLevel.getLevelWrappers().add(stdLevelWrapper);
      }
    }



    return new ArrayList<>(stdLevelMap.values());

  }

  /*
def meanDifferencesAtStandardLevels(origLevels, diffLevels, depths, parameters):
    '''
    origLevels: list of level indices under consideration
    diffLevels: list of differences corresponding to origLevels
    depths: list of depths of all levels in profile.
    returns (levels, assocLevs), where
    levels == a masked array of mean differences at each standard level
    assocLevs == a list of the indices of the closest standard levels to the levels indicated in origLevels
    '''

    # Get the set of standard levels.
    stdLevels = parameters['enbackground']['depth']

    # Create arrays to hold the standard level data and aggregate.
    nStdLevels = len(stdLevels)
    levels     = np.zeros(nStdLevels)
    nPerLev    = np.zeros(nStdLevels)
    assocLevs  = []
    for i, origLevel in enumerate(origLevels):
        # Find the closest standard level.
        j          = np.argmin(np.abs(depths[origLevel] - stdLevels))
        assocLevs.append(j)
        levels[j]  += diffLevels[i]
        nPerLev[j] += 1

    # Average the standard levels where there are data.
    iGT1 = nPerLev > 1
    levels[iGT1] /= nPerLev[iGT1]
    levels = np.ma.array(levels)
    levels.mask = False
    levels.mask[nPerLev == 0] = True

    return levels, assocLevs
   */
}
