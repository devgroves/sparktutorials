package com.java21;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import io.delta.tables.DeltaTable;

import java.util.Map;

public class DeltaLakeExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Delta Lake Example")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

// Create a Delta table
        Dataset<Row> data = spark.range(0, 5).toDF("id");
        data.write().format("delta").save("/tmp/delta-table");

// Read the Delta table
        Dataset<Row> deltaTable = spark.read().format("delta").load("/tmp/delta-table");
        deltaTable.show();

// Update the Delta table
//        DeltaTable deltaTableInstance = DeltaTable.forPath(spark, "/tmp/delta-table");
//        deltaTableInstance.updateExpr(Map.of("id = 1", "id = 10"));
//
//// Read the updated Delta table
//        deltaTable = spark.read().format("delta").load("/tmp/delta-table");
//        deltaTable.show();

        spark.stop();
    }
}