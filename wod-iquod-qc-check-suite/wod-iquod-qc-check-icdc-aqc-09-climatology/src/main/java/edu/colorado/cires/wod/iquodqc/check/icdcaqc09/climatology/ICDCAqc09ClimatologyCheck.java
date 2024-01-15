package edu.colorado.cires.wod.iquodqc.check.icdcaqc09.climatology;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.icdcaqc09.climatology.refdata.ClimatologicalTMedianAndAmdForAqcParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.math3.util.FastMath;
import org.apache.spark.sql.Row;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class ICDCAqc09ClimatologyCheck extends CommonCastCheck {
  
  private static String climatologicalTMedianAndAmdForAqc;
  
  private Properties properties;
  
  @Override
  public String getName() {
    return CheckNames.ICDC_AQC_09_CLIMATOLOGY_CHECK.getName();
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (climatologicalTMedianAndAmdForAqc == null) {
      load(properties);
    }
    return super.checkUdf(row);
  }
  
  protected static void load(Properties properties) {
    synchronized (ICDCAqc09ClimatologyCheck.class) {
      if (climatologicalTMedianAndAmdForAqc == null) {
        climatologicalTMedianAndAmdForAqc = ClimatologicalTMedianAndAmdForAqcParametersReader.loadParameters(properties);
      }
    }
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    try (NetcdfFile file = NetcdfFiles.open(climatologicalTMedianAndAmdForAqc)) {
      DepthData depthData = new DepthData(cast);

      double latitude = cast.getLatitude();
      double longitude = cast.getLongitude();

      if ((latitude >= 35.0 && latitude <= 45.0 && longitude >= 45.0 && longitude <= 60.0) ||
          (latitude >= 40.0 && latitude <= 50.0 && longitude >= -95.0 && longitude <= -75.0)) {
        return Collections.emptyList();
      }

      List<MinMax> minMaxes = getClimatologyRange(depthData, file, latitude, longitude, cast.getTimestamp());
      
      if (minMaxes == null) {
        return Collections.emptyList();
      }

      List<Integer> failedDepths = new ArrayList<>();
      for (int i = 0; i < depthData.getnLevels(); i++) {
        double temperature = depthData.getTemperatures().get(i);
        double tMin = minMaxes.get(i).getMin();
        double tMax = minMaxes.get(i).getMax();
        if ((temperature < tMin || temperature > tMax)) {
          double fillValue = (double) Objects.requireNonNull(Objects.requireNonNull(file.findGlobalAttribute("fillValue")).getValue(0));
          if (tMax != fillValue && tMin != fillValue) {
            failedDepths.add(i);
          }
        }
      }

      return failedDepths;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static @Nullable List<MinMax> getClimatologyRange(DepthData depthData, NetcdfFile file, double latitude, double longitude, long timestamp) {
    double fillValue = (double) Objects.requireNonNull(Objects.requireNonNull(file.findGlobalAttribute("fillValue")).getValue(0));
    

    int nLevels = depthData.getnLevels();
    MinMax[] output = new MinMax[nLevels];
    double[] tMin = new double[nLevels];
    double[] tMax = new double[nLevels];

    Arrays.fill(tMin, fillValue);
    Arrays.fill(tMax, fillValue);

    double parMinOver = -2.3;
    double parMaxOver = 33.0;

    int iy = (int) FastMath.floor((90.0 - latitude) / 0.5);
    int ix = (int) FastMath.floor((longitude + 180.0) / 0.5);

    if (iy < 0 || iy > 360 || ix < 0 || ix > 720) {
      return null;
    }

    for (int k = 0; k < nLevels; k++) {
      double depth = depthData.getDepths().get(k);

      Variable variable = Objects.requireNonNull(file.findVariable("zedqc"));

      int idx = 0;
      for (int i = 0; i < variable.getShape(0) - 1; i++) {
        try {
          if (depth >= Objects.requireNonNull(file.findVariable("zedqc")).read(
              Section.builder()
                  .appendRange(i, i)
                  .build()
          ).getDouble(0) && depth < variable.read(Section.builder()
                  .appendRange(i + 1, i + 1)
              .build()).getDouble(0)) {
            idx = i;
            break;
          }
        } catch (IOException | InvalidRangeException e) {
          throw new RuntimeException(e);
        }
      }
      
      boolean useAnnual = idx > 15;

      Double amd = null;
      Double median = null;
      if (!useAnnual) {
        int month = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC")).getMonthValue();

        try {
          Variable tamdM = Objects.requireNonNull(file.findVariable("tamdM"));
          
          double scaleFactor = (double) Objects.requireNonNull(Objects.requireNonNull(tamdM.findAttribute("scale_factor")).getValue(0));
          
          amd = Objects.requireNonNull(file.findVariable("tamdM")).read(
              Section.builder()
                  .appendRange(ix, ix)
                  .appendRange(iy, iy)
                  .appendRange(idx, idx)
                  .appendRange(month - 1, month - 1)
                  .build()
          ).getDouble(0) * scaleFactor;
          
          if (amd < 0) {
            useAnnual = true;
          } else {
            scaleFactor = (double) Objects.requireNonNull(
                Objects.requireNonNull(Objects.requireNonNull(file.findVariable("tmedM")).findAttribute("scale_factor")).getValue(0));
            median = Objects.requireNonNull(file.findVariable("tmedM")).read(
                Section.builder()
                    .appendRange(ix, ix)
                    .appendRange(iy, iy)
                    .appendRange(idx, idx)
                    .appendRange(month - 1, month - 1)
                    .build()
            ).getDouble(0) * scaleFactor;
            
            if (median < parMinOver) {
              useAnnual = true;
            }
          }
        } catch (IOException | InvalidRangeException e) {
          throw new RuntimeException(e);
        }
      }
      
      if (useAnnual) {
        try {

          Variable tamdA = Objects.requireNonNull(file.findVariable("tamdA")); 
          double scaleFactor = (double) Objects.requireNonNull(Objects.requireNonNull(tamdA.findAttribute("scale_factor")).getValue(0));
          
          amd = Objects.requireNonNull(file.findVariable("tamdA")).read(
              Section.builder()
                  .appendRange(ix, ix)
                  .appendRange(iy, iy)
                  .appendRange(idx, idx)
                  .build()
          ).getDouble(0) * scaleFactor;

          if (amd < 0) {
            continue;
          } else {
            Variable tmedA = Objects.requireNonNull(file.findVariable("tmedA"));
            scaleFactor = (double) Objects.requireNonNull(Objects.requireNonNull(tmedA.findAttribute("scale_factor")).getValue(0));
            median = tmedA.read(
                Section.builder()
                    .appendRange(ix, ix)
                    .appendRange(iy, iy)
                    .appendRange(idx, idx)
                    .build()
            ).getDouble(0) * scaleFactor;

            if (median < parMinOver) {
              continue;
            }
          }
        } catch (IOException | InvalidRangeException e) {
          throw new RuntimeException(e);
        }
      }
      
      if (amd > 0 && amd < 0.05) {
        amd = 0.05;
      }

      double tMaxA = median + 3.0 * amd;
      double tMinA = median - 3.0 * amd;

      tMinA = Math.max(tMinA, parMinOver);
      tMaxA = Math.min(tMaxA, parMaxOver);

      
      tMin[k] = tMinA;
      tMax[k] = tMaxA;
    }
    
    return IntStream.range(0, nLevels).boxed()
        .map(i -> new MinMax(tMin[i], tMax[i]))
        .collect(Collectors.toList());
  }
  
  private static class MinMax {
    private final double min;
    private final double max;

    private MinMax(double min, double max) {
      this.min = min;
      this.max = max;
    }

    public double getMin() {
      return min;
    }

    public double getMax() {
      return max;
    }
  }
}
