package edu.colorado.cires.wod.iquodqc.check.api;

import static org.apache.spark.sql.functions.col;

import edu.colorado.cires.wod.parquet.model.Cast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public final class CastIoUtils {
  public static final String WCS84_PROJJSON = "{\"$schema\": \"https://proj.org/schemas/v0.7/projjson.schema.json\",\"type\": \"GeographicCRS\",\"name\": \"WGS 84\",\"datum_ensemble\": {\"name\": \"World Geodetic System 1984 ensemble\",\"members\": [{\"name\": \"World Geodetic System 1984 (Transit)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1166}},{\"name\": \"World Geodetic System 1984 (G730)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1152}},{\"name\": \"World Geodetic System 1984 (G873)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1153}},{\"name\": \"World Geodetic System 1984 (G1150)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1154}},{\"name\": \"World Geodetic System 1984 (G1674)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1155}},{\"name\": \"World Geodetic System 1984 (G1762)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1156}},{\"name\": \"World Geodetic System 1984 (G2139)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1309}}],\"ellipsoid\": {\"name\": \"WGS 84\",\"semi_major_axis\": 6378137,\"inverse_flattening\": 298.257223563},\"accuracy\": \"2.0\",\"id\": {\"authority\": \"EPSG\",\"code\": 6326}},\"coordinate_system\": {\"subtype\": \"ellipsoidal\",\"axis\": [{\"name\": \"Geodetic latitude\",\"abbreviation\": \"Lat\",\"direction\": \"north\",\"unit\": \"degree\"},{\"name\": \"Geodetic longitude\",\"abbreviation\": \"Lon\",\"direction\": \"east\",\"unit\": \"degree\"}]},\"scope\": \"Horizontal component of 3D system.\",\"area\": \"World.\",\"bbox\": {\"south_latitude\": -90,\"west_longitude\": -180,\"north_latitude\": 90,\"east_longitude\": 180},\"id\": {\"authority\": \"EPSG\",\"code\": 4326}}";
  public static final String GEOPARQUET_VERSION = "1.0.0";

  public static Dataset<Row> readCastRowDataset(SparkSession spark, String... urls) {
    return spark.read().format("geoparquet").load(urls);
  }

  public static Dataset<Cast> readCastDataset(SparkSession spark, String... urls) {
    return readCastRowDataset(spark, urls).as(Encoders.bean(Cast.class));
  }

  public static void writeCastDataset(Dataset<Cast> dataset, String path) {
    dataset.repartition(col("geohash3")).sortWithinPartitions("geohash").write()
        .format("geoparquet")
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .mode(SaveMode.Overwrite)
        .partitionBy("geohash3")
        .save(path);
  }
}
