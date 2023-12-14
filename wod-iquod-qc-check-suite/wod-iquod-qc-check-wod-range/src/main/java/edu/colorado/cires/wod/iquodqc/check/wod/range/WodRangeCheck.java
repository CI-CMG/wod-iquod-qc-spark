package edu.colorado.cires.wod.iquodqc.check.wod.range;

import static edu.colorado.cires.wod.iquodqc.check.wod.range.WodRange.getMinMax;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.wod.range.WodRange.RegionMinMax;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;

public class WodRangeCheck extends CommonCastCheck {

  private static final Map<Integer, String> CELL_CODE_TO_REGION_NAME = new HashMap<>();

  static {
    CELL_CODE_TO_REGION_NAME.put(2,"North_Atlantic");
    CELL_CODE_TO_REGION_NAME.put(3,"Coastal_N_Atlantic");
    CELL_CODE_TO_REGION_NAME.put(4,"Equatorial_Atlant");
    CELL_CODE_TO_REGION_NAME.put(5,"Coastal_Eq_Atlant");
    CELL_CODE_TO_REGION_NAME.put(6,"South_Atlantic");
    CELL_CODE_TO_REGION_NAME.put(7,"Coastal_S_Atlantic");
    CELL_CODE_TO_REGION_NAME.put(8,"North_Pacific");
    CELL_CODE_TO_REGION_NAME.put(9,"Coastal_N_Pac");
    CELL_CODE_TO_REGION_NAME.put(10,"Equatorial_Pac");
    CELL_CODE_TO_REGION_NAME.put(11,"Coastal_Eq_Pac");
    CELL_CODE_TO_REGION_NAME.put(12,"South_Pacific");
    CELL_CODE_TO_REGION_NAME.put(13,"Coastal_S_Pac");
    CELL_CODE_TO_REGION_NAME.put(14,"North_Indian");
    CELL_CODE_TO_REGION_NAME.put(15,"Coastal_N_Indian");
    CELL_CODE_TO_REGION_NAME.put(16,"Equatorial_Indian");
    CELL_CODE_TO_REGION_NAME.put(17,"Coastal_Eq_Indian");
    CELL_CODE_TO_REGION_NAME.put(18,"South_Indian");
    CELL_CODE_TO_REGION_NAME.put(19,"Coastal_S_Indian");
    CELL_CODE_TO_REGION_NAME.put(20,"Antarctic");
    CELL_CODE_TO_REGION_NAME.put(21,"Arctic");
    CELL_CODE_TO_REGION_NAME.put(22,"Mediteranean");
    CELL_CODE_TO_REGION_NAME.put(23,"Black_Sea");
    CELL_CODE_TO_REGION_NAME.put(24,"Baltic_Sea");
    CELL_CODE_TO_REGION_NAME.put(25,"Persian_Gulf");
    CELL_CODE_TO_REGION_NAME.put(26,"Red_Sea");
    CELL_CODE_TO_REGION_NAME.put(27,"Sulu_Sea");
  }

  private static final RegionMinMax REGION_MIN_MAX;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    try {
      String json = FileUtils.readFileToString(new File("src/main/resources/data/WOD_ranges_Temperature.json"), StandardCharsets.UTF_8);
      REGION_MIN_MAX = OBJECT_MAPPER.readValue(json, RegionMinMax.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Map<String, Map<String, Integer>> RANGE_AREA;

  static {
    try {
      String json = FileUtils.readFileToString(new File("src/main/resources/data/range_area.json"), StandardCharsets.UTF_8);
      RANGE_AREA = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "WOD_RANGE_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return WodRange.checkWodRange(
        cast,
        getMinMax(
            cast.getLatitude(),
            cast.getLongitude(),
            RANGE_AREA,
            CELL_CODE_TO_REGION_NAME,
            REGION_MIN_MAX
        ),
        REGION_MIN_MAX.getDepths()
    );
  }
}
