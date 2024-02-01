package edu.colorado.cires.wod.postprocess;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

public class PostProcessorRunner<T extends Serializable> implements Runnable {
  
  private final PostProcessor<T> postProcessor;
  private final String outputURI;
  private final PostProcessorContext postProcessorContext;
  private final SaveMode saveMode;
  private final long maxRecordsPerFile;

  public PostProcessorRunner(PostProcessor<T> postProcessor, String outputURI, PostProcessorContext postProcessorContext, SaveMode saveMode, long maxRecordsPerFile) {
    this.postProcessor = postProcessor;
    this.outputURI = outputURI;
    this.postProcessorContext = postProcessorContext;
    this.saveMode = saveMode;
    this.maxRecordsPerFile = maxRecordsPerFile;
  }

  @Override
  public void run() {
    Dataset<T> dataset = postProcessor.process(postProcessorContext);
    DatasetIO.writeDataset(outputURI, dataset, saveMode, maxRecordsPerFile);
  }
}
