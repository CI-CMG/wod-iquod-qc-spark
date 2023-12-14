package edu.colorado.cires.wod.iquodqc.check.wod.range;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WodRange {

  public static class MinMax {
    private List<Double> min;
    private List<Double> max;

    public List<Double> getMin() {
      return min;
    }

    public void setMin(List<Double> min) {
      this.min = min;
    }

    public List<Double> getMax() {
      return max;
    }

    public void setMax(List<Double> max) {
      this.max = max;
    }
  }

  public static class RegionMinMax {

    private Map<String, MinMax> regions;
    private List<Double> depths;

    public Map<String, MinMax> getRegions() {
      return regions;
    }

    public void setRegions(Map<String, MinMax> regions) {
      this.regions = regions;
    }

    public List<Double> getDepths() {
      return depths;
    }

    public void setDepths(List<Double> depths) {
      this.depths = depths;
    }
  }

  public static class LatLng {
    private final double lat;
    private final double lng;

    private LatLng(double lat, double lng) {
      this.lat = lat;
      this.lng = lng;
    }

    public double getLat() {
      return lat;
    }

    public double getLng() {
      return lng;
    }
  }

  public static Integer getCellCode(Double lat, Double lng, Map<String, Map<String, Integer>> rangeArea) {
    return Objects.requireNonNull(
        rangeArea.get(lat.toString()).get(lng.toString()),
        String.format("Cell code not found for (%s, %s)", lat, lng)
    );
  }
  
  public static String getRegionName(Integer cellCode, Map<Integer, String> codeToRegion) {
    return Objects.requireNonNull(
        codeToRegion.get(cellCode),
        String.format("Region name not found for cell code: %s", cellCode)
    );
  }
  
  public static MinMax getMinMax(String regionName, RegionMinMax regionMinMax) {
    return Objects.requireNonNull(
        regionMinMax.regions.get(regionName),
        String.format("Region not found: %s", regionName)
    );
  }
  
  public static MinMax getMinMax(double lat, double lng, Map<String, Map<String, Integer>> rangeArea, Map<Integer, String> codeToRegion, RegionMinMax regionMinMax) {
    LatLng latLng = getGridCoordinates(lat, lng);
    return getMinMax(
        getRegionName(
            getCellCode(
                latLng.getLat(),
                latLng.getLng(),
                rangeArea
            ),
          codeToRegion
        ),
        regionMinMax
    );
  }
  
  public static LatLng getGridCoordinates(double lat, double lng) {
    return new LatLng(
        (Math.round(lat - 0.5) + 0.5 + 90) % 180 - 90,
        (Math.round(lng - 0.5) + 0.5 + 180) % 360 - 180
    );
  }
  
  public static List<Integer> checkWodRange(Cast cast, MinMax referenceMinMax, List<Double> referenceDepths) {
    List<Depth> depths = cast.getDepths();
    return IntStream.range(0, depths.size()).boxed()
        .filter(i -> depthIsInvalid(depths.get(i), referenceMinMax, referenceDepths))
        .collect(Collectors.toList());
  }
  
  private static boolean depthIsInvalid(Depth depth, MinMax referenceMinMax, List<Double> referenceDepths) {
    double depthValue = depth.getDepth();
    Double temperatureValue = getTemperature(depth).map(ProfileData::getValue)
        .orElse(Double.NaN);
    
    if (Double.isNaN(depthValue) && Double.isNaN(temperatureValue)) {
      return false;
    }
    
    int iDepth = 0;
    while (iDepth < referenceDepths.size() && depthValue > referenceDepths.get(iDepth)) {
      iDepth = iDepth + 1;
    }

    return temperatureValue < referenceMinMax.getMin().get(iDepth) || temperatureValue > referenceMinMax.getMax().get(iDepth);
  }

}
