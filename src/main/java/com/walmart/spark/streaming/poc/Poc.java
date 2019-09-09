package com.walmart.spark.streaming.poc;

import com.walmart.spark.streaming.poc.domain.PersonInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/***
 *
 * Reporting Metrics using Dropwizard https://spark.apache.org/docs/latest/structured-streaming-programming-guide
 * .html#reporting-metrics-using-dropwizard
 *
 * */
public class Poc {


    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "/Users/staleti/hadoop");
        //Logger.getLogger("org.apache").setLevel(Level.WARN);
        //Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("spark-streaming-poc")
            //.enableHiveSupport()
            .getOrCreate();

        // We can experiment with this to make the batch run faster
        // session.conf().set("spark.sql.shuffle.partitions", "10");

        Encoder<PersonInfo> schema = Encoders.bean(PersonInfo.class);

        Dataset<Row> df = session.readStream()
            .option("badRecordsPath", "/Users/staleti/hadoop/badRecordsPath")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema.schema())
            .json("/Users/staleti/Walmart-Projects/data/stream-poc-1")
            .coalesce(4)
            .toDF();

        df.printSchema();

        // df.dropDuplicates()
        //df.show();

        // start some dataframe operations
        df.createOrReplaceTempView("poc_json_view");

        // key, value, timestamp
        Dataset<Row> invalidMessages =
            session.sql("select _corrupted from poc_json_view");

        Dataset<Row> validMessages =
            session.sql("select * from poc_json_view");

        StreamingQuery query = validMessages
            .writeStream()
            .format("parquet")
            .partitionBy("dob")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", "/Users/staleti/hadoop")
            .option("path", "/Users/staleti/hadoop/data")
            //.option("truncate", false)
            //.option("numRows", 50)
            .start();

        query.awaitTermination();
    }
}
