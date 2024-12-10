package com.java21;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.hadoop.*;

import java.net.URI;
import java.util.*;

public class SparkConnectivity {

        public static void main(String[] args) {
            System.out.println("spark connectivity...");
// Create a Spark session
            SparkSession spark = SparkSession.builder()
                    .appName("Spark Connectivity Test")
                    .master("spark://172.17.0.2:7077")
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
                        new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("salary", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("dept", DataTypes.StringType, false, Metadata.empty())
                });

// Create data
                Row row1 = RowFactory.create(1, "John", 30.50d, "Dept1");
                Row row2 = RowFactory.create(2, "Jane", 45.50d, "Dept1");
                Row row3 = RowFactory.create(3, "Doe", 90.50d, "Dept2");

                List<Row> data = Arrays.asList(row1, row2, row3);

// Create DataFrame
                Dataset<Row> df = spark.createDataFrame(data, schema);
                df.show();
                df.groupBy("dept").sum("salary").show();


                try {
                    // Hadoop configuration
                    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
                    FileSystem fs = FileSystem.get(URI.create("file:///"), hadoopConf);

// Path to the directory
                    Path path = new Path("./");

// List files in the directory
                    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, false);
                    System.out.println("file status list iterator " + fileStatusListIterator);
                    while (fileStatusListIterator.hasNext()) {
                        LocatedFileStatus fileStatus = fileStatusListIterator.next();
                        System.out.println(fileStatus.getPath().toString());
                        if (fileStatus.getPath().toString().contains(".csv") && fs.exists(fileStatus.getPath())) {
                            Dataset<String> logData = spark.read().text(fileStatus.getPath().toString()).toJSON();
                            System.out.println(logData.count());
                        }
                    }

                    Dataset<Row> csvdf = spark.read()
                            .option("header", "true")
                            .option("encoding", "UTF-8").load("test.csv");
                    System.out.println("csv data frame: " +  csvdf);
                    csvdf.show();
                } catch (Exception exception) {
                    System.out.println("exception happened "+ exception.getMessage());
                }

            }

// Stop the Spark session
            spark.stop();
        }
}
