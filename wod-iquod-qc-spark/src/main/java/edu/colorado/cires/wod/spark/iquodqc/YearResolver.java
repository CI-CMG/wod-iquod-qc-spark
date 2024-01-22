package edu.colorado.cires.wod.spark.iquodqc;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class YearResolver {

  public static List<Integer> resolveYears(List<Integer> providedYears, S3Client s3, String bucket, String keyPrefix, String dataset, String processingLevel) {
    if (providedYears == null || providedYears.isEmpty()) {
      Pattern pattern = Pattern.compile(".*" + dataset + processingLevel.charAt(0) + "(\\d\\d\\d\\d)" + "\\.parquet/_SUCCESS$");
      return listObjects(s3, bucket, keyPrefix == null ? "" : keyPrefix, (key) -> {
        Matcher matcher = pattern.matcher(key);
        return matcher.matches();
      }).stream().map(key -> {
        Matcher matcher = pattern.matcher(key);
        matcher.matches();
        return Integer.parseInt(matcher.group(1));
      }).sorted().collect(Collectors.toList());
    } else {
      return providedYears;
    }
  }

  private static Set<String> listObjects(S3Client s3, String bucket, String keyPrefix, Predicate<String> filter) {
    Set<String> keys = new TreeSet<>();
    for (ListObjectsV2Response page : s3.listObjectsV2Paginator(c -> c.bucket(bucket).prefix(keyPrefix))) {
      keys.addAll(page.contents().stream().map(S3Object::key).filter(filter).collect(Collectors.toList()));
    }
    return keys;
  }

}
