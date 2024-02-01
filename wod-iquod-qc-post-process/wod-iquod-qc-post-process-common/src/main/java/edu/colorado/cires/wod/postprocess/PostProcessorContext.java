package edu.colorado.cires.wod.postprocess;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import org.apache.spark.sql.Dataset;

public interface PostProcessorContext {
  
  Dataset<Cast> readCastDataset();
  Dataset<CastCheckResult> readCheckResultDataset();

}
