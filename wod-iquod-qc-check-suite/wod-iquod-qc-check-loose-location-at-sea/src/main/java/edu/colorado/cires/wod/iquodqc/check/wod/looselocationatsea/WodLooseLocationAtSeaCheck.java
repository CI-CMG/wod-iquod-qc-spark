package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class WodLooseLocationAtSeaCheck extends CommonCastCheck {
  
  private static final int BUFFER_WIDTH = 2;

  @Override
  public String getName() {
    return "WOD_LOOSE_LOCATION_AT_SEA_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    try (NetcdfFile netcdfFile = NetcdfFiles.open("src/main/resources/data/etopo5.nc")) {
      if (WodLooseLocationAtSea.checkLooseLocationAtSea(cast.getLatitude(), cast.getLongitude(), BUFFER_WIDTH, netcdfFile)) {
        return Collections.emptyList();
      }
      return IntStream.range(0, cast.getDepths().size())
          .boxed()
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
