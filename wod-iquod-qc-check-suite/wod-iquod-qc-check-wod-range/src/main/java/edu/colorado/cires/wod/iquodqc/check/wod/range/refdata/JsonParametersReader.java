package edu.colorado.cires.wod.iquodqc.check.wod.range.refdata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.wod.iquodqc.check.wod.range.WodRange.RegionMinMax;
import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;

public class JsonParametersReader {
  
  public static final String WOD_RANGE_AREA_PROP = "wod_range_area.json.uri";
  public static final String WOD_RANGES_TEMPERATURE_PROP = "wod_ranges_temperature.json.uri";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  public static Map<String, Map<String, Integer>> openRangeArea(Properties properties) {
    return FileDownloader.loadParameters(properties, WOD_RANGE_AREA_PROP, (jsonFile) -> {
      try {
        String json = FileUtils.readFileToString(jsonFile.toFile(), StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
  
  public static RegionMinMax openRangesTemperature(Properties properties) {
    return FileDownloader.loadParameters(properties, WOD_RANGES_TEMPERATURE_PROP, (jsonFile) -> {
      try {
        String json = FileUtils.readFileToString(jsonFile.toFile(), StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readValue(json, RegionMinMax.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
