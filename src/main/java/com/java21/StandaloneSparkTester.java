package com.java21;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.spark.sql.functions.col;

public class StandaloneSparkTester {

        public static void main(String[] args) {
            System.out.println("spark connectivity...");
// Create a Spark session
            SparkSession spark = SparkSession.builder()
                    .appName("Spark Connectivity Test")
                    .master("local[*]")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.memory", "2g")
                    .getOrCreate();
            System.out.println("spark connectivity...");
            System.out.println(spark.catalog());
//            spark.conf().set("spark.scheduler.listenerbus.eventqueue.size", "200000");
            System.out.println(spark.sparkContext().isStopped());
            if (!spark.sparkContext().isStopped()) {
// Perform a simple operation to test connectivity
                System.out.println(spark.emptyDataFrame());
                // Define the schema
                StructType schema = new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("product", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("price", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("category", DataTypes.StringType, false, Metadata.empty())
                });

// Create data
                Row row1 = RowFactory.create(1, "14inch Laptop", 30.50d, "Compute Devices");
                Row row2 = RowFactory.create(2, "15inch Desktop", 45.50d, "Compute Devices");
                Row row3 = RowFactory.create(3, "Ink Jet Printer", 90.50d, "Accessories");

                List<Row> data = Arrays.asList(row1, row2, row3);

// Create DataFrame
                Dataset<Row> df = spark.createDataFrame(data, schema);
                df.summary().show();
                df.show();
                Dataset<Row> aggdf = df.groupBy("category").sum("price");
                aggdf.show();

                try {
                    Dataset<Row> csvdf = spark.read()
                            .option("header", "true")
                            .option("encoding", "UTF-8").csv("/home/nagappan/Downloads/out.csv");
                    printSchemaColNames(csvdf);
                    csvdf.show();
                    Dataset<Row> filteredDF = csvdf.filter(col("BeginDateTime")
                                    .cast("timestamp").gt("2023-03-07T13:00:49"));

                    System.out.println(Arrays.asList(csvdf.columns()));
                    filteredDF.show();
                    Dataset<Row> jsondf = spark.read().json("/home/nagappan/Downloads/gl-sast-report.json");
                    System.out.println("json file read and columns are --->" + Arrays.toString(jsondf.columns()));
//                    jsondf.selectExpr("scan.analyzer.vendor.name", "scan.analyzer.id" ,
//                            "scan.analyzer.name" ,
//                            "scan.analyzer.url").show();
                    jsondf.show();
//                    System.out.println(Arrays.asList(jsondf.columns()));
//                    printSchemaColNames(jsondf);
//                    Dataset<Row> flattenedRows = flattenSchema(jsondf, "");
                    List<Column> colList = flattenColList(jsondf);
                    System.out.println("*********flattend columns********");
                    jsondf.select(colList.toArray(new Column[0])).show();
                    System.out.println("*********flattend columns********");
//                    flattenedRows.show();
                } catch (Exception exception) {
                    System.out.println("exception happened "+ exception.getMessage());
                    exception.printStackTrace();
                }

            }

// Stop the Spark session
            spark.stop();
        }

        public static void printSchemaColNames(Dataset<Row> df) {
            // Get the schema of the DataFrame
            StructType schema = df.schema();
            System.out.println("getting the print schema col names");
// Print column names and their data types
            for (StructField field : schema.fields()) {
                System.out.println("Column: " + field.name() + ", Type: " + field.dataType());
            }
        }

    public static List<Column> flattenColList(Dataset<Row> df) {
        StructType schema = df.schema();
        List<Column> columns = new ArrayList<>();
        ConcurrentLinkedQueue<StructField> metadataFields = new ConcurrentLinkedQueue<>(Arrays.asList(schema.fields()));
        for (StructField field : metadataFields) {
            System.out.println("col names -" + field.name());
            if (field.dataType() instanceof StructType) {
//                for (StructField nestedField : ((StructType) field.dataType()).fields()) {
//                    columns.add(col(field.name() + "." + nestedField.name()).alias(field.name() + "." + nestedField.name()));
//                }
                List<StructField> ancestors = new ArrayList<>();
                columns.addAll(nestedColumns(field, ancestors));
            } else {
                columns.add(col(field.name()));
            }
        }
        return columns;
    }

    public static List<Column> nestedColumns(StructField field, List<StructField> parentField) {
        System.out.println("entered nested columns " + field + " ancestors " + parentField  );
        List<Column> columns = new ArrayList<>();
        for (StructField nestedField : ((StructType) field.dataType()).fields()) {
            System.out.println("col names -" + nestedField.name() + " parent field "+ field.name());
            if (nestedField.dataType() instanceof StructType) {
                List<StructField> parentCopy = new ArrayList<>(parentField); parentCopy.add(field);
                columns.addAll(nestedColumns(nestedField, parentCopy));
            } else {
                String colName = "";
                for (StructField element: parentField) {
                    colName += element.name() + ".";
                }
                colName +=field.name() + "." + nestedField.name();
                System.out.println("complete field path --->" + colName);
                columns.add(col(colName).alias(colName));
            }
        }
        return columns;
    }

    public static Dataset<Row> flattenSchema(Dataset<Row> df, String prefix) {
        List<String> columns = new ArrayList<>();
            try {

                for (StructField field : df.schema().fields()) {
                    String fieldName = prefix + field.name();
                    System.out.println(" field name " + fieldName);
                    if (field.dataType() instanceof StructType) {
                        Dataset<Row> nestedDF = df.select(col(field.name() + ".*"));
                        nestedDF.show();
                        Dataset<Row> flattenedNestedDF = flattenSchema(nestedDF, fieldName + ".");
                        System.out.println("flattened columns " + Arrays.asList(flattenedNestedDF.columns()));
                        for (String nestedField : flattenedNestedDF.columns()) {
                            columns.add(nestedField);
                        }
                        System.out.println("selct expression : " + df.selectExpr(columns.toArray(new String[0])));
                    } else if (field.dataType() instanceof ArrayType) {
//                        String explodedFieldName = prefix + fieldName ;
//                        Dataset<Row> explodedDF = df.withColumn(explodedFieldName, functions.explode(functions.col(fieldName)));
//                        Dataset<Row> flattenedExplodedDF = flattenSchema(explodedDF, explodedFieldName + ".");
//                        for (String nestedField : flattenedExplodedDF.columns()) {
//                            columns.add(nestedField);
//                        }
                    } else {
                        columns.add(field.name());
                    }
                }
            } catch (Exception exception) {
                System.out.println(exception.getCause());
            }
        System.out.println("columns -" + columns);
        return df.selectExpr(columns.toArray(new String[0]));
    }

}
