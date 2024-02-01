package edu.colorado.cires.wod.postprocess;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

public class PostProcessorExecutor<T extends Serializable> {
  
  private final PostProcessor<T> postProcessor;

  public PostProcessorExecutor(PostProcessor<T> postProcessor) {
    this.postProcessor = postProcessor;
  }
  
  public void execute(String outputURI, PostProcessorContext postProcessorContext, SaveMode saveMode, long maxRecordsPerFile) {
    Dataset<T> dataset = postProcessor.process(postProcessorContext);
    DatasetIO.writeDataset(outputURI, dataset, saveMode, maxRecordsPerFile);
  }
}
