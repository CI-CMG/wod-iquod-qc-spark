package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata;

import java.nio.file.Path;

public class CarsParameters {

  private final Path dataFilePath;

  public CarsParameters(Path dataFilePath) {
    this.dataFilePath = dataFilePath;
  }

  public Path getDataFilePath() {
    return dataFilePath;
  }
}
