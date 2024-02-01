package edu.colorado.cires.wod.postprocess;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.Serializable;
import org.apache.spark.sql.Dataset;

public abstract class PostProcessor<T extends Serializable> implements Serializable {
  
  private static final long serialVersionUID = 0L;
  
  protected abstract Dataset<T> processDatasets(Dataset<Cast> castDataset, Dataset<CastCheckResult> castCheckResultDataset);
  
  public Dataset<T> process(PostProcessorContext postProcessorContext) {
    Dataset<Cast> castDataset = postProcessorContext.readCastDataset();
    Dataset<CastCheckResult> checkResultDataset = postProcessorContext.readCheckResultDataset();
    
    return processDatasets(castDataset, checkResultDataset);
  }

}
