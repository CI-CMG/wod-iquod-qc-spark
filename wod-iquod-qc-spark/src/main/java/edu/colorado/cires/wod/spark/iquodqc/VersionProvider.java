package edu.colorado.cires.wod.spark.iquodqc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import picocli.CommandLine.IVersionProvider;

public class VersionProvider implements IVersionProvider {

  private static String version;

  @Override
  public String[] getVersion() throws Exception {
    if (version == null) {
      try (BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("version.txt")))) {
        version = in.readLine();
      }
    }
    return new String[]{version};
  }

}
