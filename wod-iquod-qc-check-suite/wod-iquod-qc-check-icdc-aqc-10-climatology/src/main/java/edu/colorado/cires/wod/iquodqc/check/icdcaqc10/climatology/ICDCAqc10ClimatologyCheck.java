package edu.colorado.cires.wod.iquodqc.check.icdcaqc10.climatology;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.icdcaqc10.climatology.refdata.GlobalMedianQuartilesMedcoupleSmoothedParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.math3.util.FastMath;
import org.apache.spark.sql.Row;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class ICDCAqc10ClimatologyCheck extends CommonCastCheck {
  
  private static String globalMedianQuartilesMedcoupleSmoothed;
  
  private Properties properties;

  @Override
  public String getName() {
    return "ICDC_AQC_10_CLIMATOLOGY_CHECK";
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (globalMedianQuartilesMedcoupleSmoothed == null) {
      load(properties);
    }
    return super.checkUdf(row);
  }
  
  protected static void load(Properties properties) {
    synchronized (ICDCAqc10ClimatologyCheck.class) {
      if (globalMedianQuartilesMedcoupleSmoothed == null) {
        globalMedianQuartilesMedcoupleSmoothed = GlobalMedianQuartilesMedcoupleSmoothedParametersReader.loadParameters(properties);
      }
    }
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Integer> failedDepths = new ArrayList<>();
    
    try (NetcdfFile file = NetcdfFiles.open(globalMedianQuartilesMedcoupleSmoothed)) {
      int[] gridShape = Objects.requireNonNull(file.findVariable("tmin_annual")).getShape();

      int j = (int) FastMath.round((90 - cast.getLatitude()) / 0.5);
      
      if (j < 0 || j >= gridShape[1]) {
        return Collections.emptyList();
      }

      double lon = cast.getLongitude();
      if (lon >= 180.0 && lon < 360.0) {
        lon -= 360.0;
      }
      
      int i = (int) FastMath.round((lon + 180.0) / 0.5);
      if (i < 0 || i >= gridShape[2]) {
        return Collections.emptyList();
      }

      double fillValue = Objects.requireNonNull(Objects.requireNonNull(Objects.requireNonNull(file.findVariable("tmin_monthly")).findAttribute("_FillValue")).getValues()).getDouble(0);
      
      for (int k = 0; k < cast.getDepths().size(); k++) {
        Depth depth = cast.getDepths().get(k);
        if (!validDepth(depth)) {
          continue;
        }
        
        int climateDepth = -1;
        Variable variable = Objects.requireNonNull(file.findVariable("deptha"));
        for (int l = 0; l < variable.getShape()[0] - 1; l++) {
          double currentValue = variable.read(Section.builder().appendRange(l, l).build()).getDouble(0);
          double nextValue = variable.read(Section.builder().appendRange(l + 1, l + 1).build()).getDouble(0);

          double depthValue = depth.getDepth();
          if (depthValue >= currentValue && depthValue < nextValue) {
            climateDepth = l;
          }
        }

        if (climateDepth == -1) {
          continue;
        }

        boolean useAnnual = false;
        Double tMin1 = null;
        Double tMin2 = null;
        Double tMax1 = null;
        Double tMax2 = null;

        if (climateDepth < 37) {
          int month = LocalDateTime.ofInstant(Instant.ofEpochMilli(cast.getTimestamp()), ZoneId.of("UTC")).getMonthValue();

          tMin1 = Objects.requireNonNull(file.findVariable("tmin_monthly")).read(Section.builder()
              .appendRange(month - 1, month - 1)
              .appendRange(climateDepth, climateDepth)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMin2 = Objects.requireNonNull(file.findVariable("tmin_monthly")).read(Section.builder()
              .appendRange(month - 1, month - 1)
              .appendRange(climateDepth + 1, climateDepth + 1)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMax1 = Objects.requireNonNull(file.findVariable("tmax_monthly")).read(Section.builder()
              .appendRange(month - 1, month - 1)
              .appendRange(climateDepth, climateDepth)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMax2 = Objects.requireNonNull(file.findVariable("tmax_monthly")).read(Section.builder()
              .appendRange(month - 1, month - 1)
              .appendRange(climateDepth + 1, climateDepth + 1)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          if (tMin1 == fillValue || tMin2 == fillValue || tMax1 == fillValue || tMax2 == fillValue) {
            useAnnual = true;
          }
        }

        if (climateDepth >= 37 || useAnnual) {
          tMin1 = Objects.requireNonNull(file.findVariable("tmin_annual")).read(Section.builder()
              .appendRange(climateDepth, climateDepth)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMin2 = Objects.requireNonNull(file.findVariable("tmin_annual")).read(Section.builder()
              .appendRange(climateDepth + 1, climateDepth + 1)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMax1 = Objects.requireNonNull(file.findVariable("tmax_annual")).read(Section.builder()
              .appendRange(climateDepth, climateDepth)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);

          tMax2 = Objects.requireNonNull(file.findVariable("tmax_annual")).read(Section.builder()
              .appendRange(climateDepth + 1, climateDepth + 1)
              .appendRange(j, j)
              .appendRange(i, i)
              .build()).getDouble(0);
        }

        double parMaxOver = 33.0;
        double parMinOver = -2.0;

        if (tMin1 == fillValue || tMin2 == fillValue || tMax1 == fillValue || tMax2 == fillValue) {
          tMin1 = parMinOver;
          tMin2 = parMinOver;
          tMax1 = parMaxOver;
          tMax2 = parMaxOver;
        }

        double tMin = FastMath.min(tMin1, tMin2);
        double tMax = FastMath.max(tMax1, tMax2);
        
        double temperature = getTemperature(depth).orElseThrow(
            () -> new IllegalStateException("Depth has no temperature value")
        ).getValue();
        
        if (temperature < tMin || temperature > tMax) {
          failedDepths.add(k);
        }
      }
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }

    return failedDepths;
  }
  
  private static boolean validDepth(Depth depth) {
    Optional<ProfileData> maybeProfileData = getTemperature(depth);
    return !Double.isNaN(depth.getDepth()) && maybeProfileData.isPresent() && !Double.isNaN(maybeProfileData.get().getValue());
  }
}
