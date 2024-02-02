package edu.colorado.cires.wod.postprocess;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import scala.Function4;

public class PostProcessorRunner<T extends Serializable> implements Runnable {
  
  private final PostProcessor<T> postProcessor;
  private final String outputURI;
  private final PostProcessorContext postProcessorContext;
  private final SaveMode saveMode;
  private final long maxRecordsPerFile;
  private Function4<String, Dataset<T>, SaveMode, Long, Boolean> writeMethod;

  public PostProcessorRunner(PostProcessor<T> postProcessor, String outputURI, PostProcessorContext postProcessorContext, SaveMode saveMode, long maxRecordsPerFile) {
    this.postProcessor = postProcessor;
    this.outputURI = outputURI;
    this.postProcessorContext = postProcessorContext;
    this.saveMode = saveMode;
    this.maxRecordsPerFile = maxRecordsPerFile;
    this.writeMethod = DatasetIO::writeDataset;
  }
  
  public void usePartitioningSaveMethod(String[] partitionFields) {
    this.writeMethod = (outputURI, dataset, saveMode, maxRecordsPerFile) -> 
        DatasetIO.writeDatasetInPartitions(outputURI, dataset, saveMode, maxRecordsPerFile, partitionFields);
  }

  @Override
  public void run() {
    Dataset<T> dataset = postProcessor.process(postProcessorContext);
    writeMethod.apply(outputURI, dataset, saveMode, maxRecordsPerFile);
  }

}
