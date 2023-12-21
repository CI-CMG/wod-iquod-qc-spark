package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.nio.file.Path;

public class WoaParameters {

  private final Path s1Path;
  private final Path s2Path;
  private final Path s3Path;
  private final Path s4Path;

  public WoaParameters(Path s1Path, Path s2Path, Path s3Path, Path s4Path) {
    this.s1Path = s1Path;
    this.s2Path = s2Path;
    this.s3Path = s3Path;
    this.s4Path = s4Path;
  }

  public Path getS1Path() {
    return s1Path;
  }

  public Path getS2Path() {
    return s2Path;
  }

  public Path getS3Path() {
    return s3Path;
  }

  public Path getS4Path() {
    return s4Path;
  }
}
