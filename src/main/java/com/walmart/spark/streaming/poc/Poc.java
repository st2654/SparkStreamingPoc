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
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("spark-streaming-poc")
            .getOrCreate();

        // We can experiment with this to make the batch run faster
        // session.conf().set("spark.sql.shuffle.partitions", "10");

        StructType jsonSchema = new StructType()
            .add("name", "string")
            .add("job", "string ")
            .add("city", "string ")
            .add("state", "string ")
            .add("dob", "string ")
            .add("appointments", new ArrayType());

        /*// The schema is encoded in a string
        String schemaString = "name job city state dob";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
*/

      /*  Dataset<Row> df = session.readStream()
            .schema(jsonSchema)
            .format("json")
            .option("path", "/Users/staleti/Walmart-Projects/data/stream-poc")
            .load();*/

        Encoder<PersonInfo> schema = Encoders.bean(PersonInfo.class);

        Dataset<Row> df = session.readStream()
            .schema(schema.schema())
            .json("/Users/staleti/Walmart-Projects/data/stream-poc")
            .coalesce(4)
            .toDF();

        df.printSchema();

        // start some dataframe operations
        df.createOrReplaceTempView("poc_json_view");

        // key, value, timestamp
        Dataset<Row> results =
            session.sql("select * from poc_json_view");

  /*      StreamingQuery query = results
            .writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            //.option("checkpointLocation", "/Users/staleti/hadoop")
            //.option("truncate", false)
            //.option("numRows", 50)
            .start();*/

        StreamingQuery query = results
            .writeStream()
            .format("parquet")
            .partitionBy("dob")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", "/Users/staleti/hadoop")
            .option("path", "/Users/staleti/hadoop/data")
            //.option("truncate", false)
            //.option("numRows", 50)
            .start();


        Dataset<Row> parquetDf = session.read()
            .schema(schema.schema())
            .parquet("/Users/staleti/hadoop/data");

        parquetDf.show();
/*
        df.createOrReplaceTempView("parquet_file_view");


        Dataset<Row> parquetResults =
            session.sql("select * from parquet_file_view");

        parquetResults.*/

        /*parquetDf.write()
            .format("parquet")
            .option("path", "/Users/staleti/hadoop/data/pp")
            .partitionBy()
            .save();*/


        query.awaitTermination();
    }
}
