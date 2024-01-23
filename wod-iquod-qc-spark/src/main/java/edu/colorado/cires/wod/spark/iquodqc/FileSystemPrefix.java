package edu.colorado.cires.wod.spark.iquodqc;

public class FileSystemPrefix {

  public static String resolve(FileSystemType fs){
    switch (fs) {
      case s3:
        return "s3a://";
      case local:
        return "file://";
      case emrS3:
        return "s3://";
      default:
        throw new IllegalStateException("Unsupported file system type, " + fs);
    }
  }

}
