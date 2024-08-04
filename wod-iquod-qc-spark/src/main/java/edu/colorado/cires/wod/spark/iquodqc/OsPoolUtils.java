package edu.colorado.cires.wod.spark.iquodqc;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "os-pool-utils",
    description = "Command line utilities for running jobs in OSPool",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {
        OsPoolDagGenerator.class
    }
)
public class OsPoolUtils implements Runnable {

  @Spec
  private CommandSpec spec;


  public static void main(String[] args) {
    System.exit(new CommandLine(new OsPoolUtils()).execute(args));
  }

  public void run() {
    spec.commandLine().usage(System.out);
  }
}
