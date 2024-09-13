package edu.colorado.cires.wod.spark.iquodqc;

import com.google.common.annotations.VisibleForTesting;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.spark.iquodqc.CheckResolver.ParentChildren;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(
    name = "generate-dag",
    description = "Generates an OSPool DAG file",
    mixinStandardHelpOptions = true)
public class OsPoolDagGenerator implements Runnable {

  @Option(names = {"-l", "--list-file"}, required = true, description = "The list file from WOD ASCII conversion")
  private Path listFile;
  @Option(names = {"-o", "--output-file"}, required = true, description = "The dag file to create")
  private Path outputFile;
  @Option(names = {"-osdf", "--osdf-prefix"}, required = true, description = "The OSDF url prefix")
  private String osdfPrefix;
  @Option(names = {"-df", "--date-folder"}, required = true, description = "The date folder")
  private String dateFolder;
  @Option(names = {"-pf", "--prune-list-file"}, description = "A CSV file with year,dataset,check to prune from the DAG")
  private Path pruneFile;
  @Option(names = {"-pd", "--prune-directory"}, description = "A directory to scan for completed checks in order prune the DAG")
  private Path pruneDir;
  @Option(names = {"-t", "--type"}, required = true, defaultValue = "qc", description = "The type of dag to generate. Either 'qc' or 'failure' - Default: ${DEFAULT-VALUE}")
  private String dagType;

  @VisibleForTesting
  void setDagType(String dagType) {
    this.dagType = dagType;
  }

  @VisibleForTesting
  void setPruneDir(Path pruneDir) {
    this.pruneDir = pruneDir;
  }

  @VisibleForTesting
  void setPruneFile(Path pruneFile) {
    this.pruneFile = pruneFile;
  }

  @VisibleForTesting
  void setListFile(Path listFile) {
    this.listFile = listFile;
  }

  @VisibleForTesting
  void setOutputFile(Path outputFile) {
    this.outputFile = outputFile;
  }

  @VisibleForTesting
  void setOsdfPrefix(String osdfPrefix) {
    this.osdfPrefix = osdfPrefix;
  }

  @VisibleForTesting
  void setDateFolder(String dateFolder) {
    this.dateFolder = dateFolder;
  }

  private Set<DatasetYear> getAll()  {
    try {
      List<String> lines = Files.readAllLines(listFile, StandardCharsets.UTF_8);
      return new TreeSet<>(lines.stream()
          .filter(StringUtils::isNotBlank)
          .map(StringUtils::trim)
          .map(line -> {
            String[] split = line.split(",");
            return new DatasetYear(split[1], split[0]);
          })
          .collect(Collectors.toSet()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<DagPruneEntry> getPrunes() {
    if (pruneFile == null && pruneDir == null) {
      return Collections.emptySet();
    }
    if (pruneFile != null && pruneDir != null) {
      throw new IllegalArgumentException("Both prune file and prune directory were specified. Only one is supported.");
    }
    try {
      if (pruneFile != null) {
        List<String> lines = Files.readAllLines(pruneFile, StandardCharsets.UTF_8);
        return Collections.unmodifiableSet(lines.stream()
            .filter(StringUtils::isNotBlank)
            .map(StringUtils::trim)
            .map(line -> {
              String[] split = line.split(",");
              return new DagPruneEntry(split[0], split[1], split[2]);
            }).collect(Collectors.toSet()));
      } else {
        try(Stream<Path> pathStream = Files.walk(pruneDir)) {
          Set<DagPruneEntry> prunes = pathStream
              .filter(Files::isRegularFile)
              .filter((file) -> file.getFileName().toString().endsWith(".parquet.tar.gz"))
              .map((file) -> {
                List<String> parts = new ArrayList<>();
                for (Path part : file) {
                  parts.add(part.getFileName().toString());
                }
                String check = parts.get(parts.size() - 1).replaceAll("\\.parquet\\.tar\\.gz$", "");
                String year = parts.get(parts.size() - 2);
                String dataset = parts.get(parts.size() - 3);
                return new DagPruneEntry(year, dataset, check);
              }).collect(Collectors.toSet());
          return Collections.unmodifiableSet(prunes);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getJobName(DatasetYear datasetYear, String check) {
    return datasetYear.dataset + "_" + datasetYear.year + "_" + check;
  }


  private String toOsdfUrl(String check, String year, String dataset) {
    return osdfPrefix + "/" + dateFolder + "/data/qc/" + dataset + "/" + year + "/" + check + ".parquet.tar.gz";
  }

  private String toOsdfUrl(String year, String dataset) {
    return osdfPrefix + "/" + dateFolder + "/data/qc/" + dataset + "/" + year + "?recursive";
  }

  private String generateDependsOn(ParentChildren pc, String year, String dataset) {
    String dependsOn = String.join(",", pc.getDependsOn().stream().map((check) -> toOsdfUrl(check, year, dataset)).collect(Collectors.toList()));
    if (dependsOn.length() > 0) {
      dependsOn = "," + dependsOn;
    }
    return dependsOn;
  }

  private void generateQcDag(OutputStream outputStream, Set<DatasetYear> all) throws IOException {
    Set<DagPruneEntry> prunes = getPrunes();
    Map<DatasetYear, List<ParentChildren>> parentChildrenMap = new HashMap<>();
    for (DatasetYear datasetYear : all) {
      List<ParentChildren> parentChildren = CheckResolver.getParentChildren(
          Collections.singleton(CheckNames.IQUOD_FLAGS_CHECK.getName()),
          datasetYear.year,
          datasetYear.dataset,
          prunes
      );
      parentChildrenMap.put(datasetYear, parentChildren);
      for (ParentChildren pc : parentChildren) {
        String jobName = getJobName(datasetYear, pc.getParent());
        outputStream.write(("JOB " + jobName + " wod-iquod-qc-spark.submit\n").getBytes(StandardCharsets.UTF_8));
        outputStream.write(("VARS " + jobName + " "
            + "dataset=\"" + datasetYear.dataset + "\" "
            + "year=\"" + datasetYear.year + "\" "
            + "check=\"" + pc.getParent() + "\" "
            + "date_folder=\"" + dateFolder + "\" "
            + "dependsOn=\"" + generateDependsOn(pc, datasetYear.year, datasetYear.dataset) + "\"\n"
        ).getBytes(StandardCharsets.UTF_8));
      }
    }
    for (DatasetYear datasetYear : all) {
      List<ParentChildren> parentChildren = parentChildrenMap.get(datasetYear);
      for (ParentChildren pc : parentChildren) {
        if (!pc.getChildren().isEmpty()) {
          String jobName = getJobName(datasetYear, pc.getParent());
          outputStream.write(("PARENT " + jobName + " CHILD").getBytes(StandardCharsets.UTF_8));
          for (String child : pc.getChildren()) {
            outputStream.write((" " + getJobName(datasetYear, child)).getBytes(StandardCharsets.UTF_8));
          }
          outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  private void generateFailuresDag(OutputStream outputStream, Set<DatasetYear> all) throws IOException {
    for (DatasetYear datasetYear : all) {
      String jobName = datasetYear.dataset + "_" + datasetYear.year;
      outputStream.write(("JOB " + jobName + " wod-iquod-failures-json-spark.submit\n").getBytes(StandardCharsets.UTF_8));
      outputStream.write(("VARS " + jobName + " "
          + "dataset=\"" + datasetYear.dataset + "\" "
          + "year=\"" + datasetYear.year + "\" "
          + "dependsOn=\"" + toOsdfUrl(datasetYear.year, datasetYear.dataset) + "\"\n"
      ).getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public void run() {
    try(OutputStream outputStream = Files.newOutputStream(outputFile)) {
      Set<DatasetYear> all = getAll();
      if (dagType.equals("qc")) {
        generateQcDag(outputStream, all);
      } else if (dagType.equals("failures")) {
        generateFailuresDag(outputStream, all);
      } else {
        throw new IllegalArgumentException("Unsupported dag type: " + dagType);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DatasetYear implements Comparable<DatasetYear> {
    private final String dataset;
    private final String year;

    private DatasetYear(String dataset, String year) {
      this.dataset = dataset;
      this.year = year;
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DatasetYear that = (DatasetYear) o;
      return Objects.equals(year, that.year) && Objects.equals(dataset, that.dataset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(year, dataset);
    }

    @Override
    public String toString() {
      return "DatasetYear{" +
          "dataset='" + dataset + '\'' +
          ", year='" + year + '\'' +
          '}';
    }

    public String toLine() {
      return year + "," + dataset + "\n";
    }

    @Override
    public int compareTo(@NotNull DatasetYear o) {
      return toString().compareTo(o.toString());
    }
  }
}
