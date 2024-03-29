package com.walmart.spark.streaming.poc;

import com.walmart.spark.streaming.poc.domain.PersonInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/***
 *
 * Reporting Metrics using Dropwizard https://spark.apache.org/docs/latest/structured-streaming-programming-guide
 * .html#reporting-metrics-using-dropwizard
 *
 * */
public class Poc {

    public static final AsyncHttpClient client = Dsl.asyncHttpClient();

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "/Users/staleti/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        BoundRequestBuilder spotlightRequest = client.preparePost("http://localhost:8080/post");

        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("spark-streaming-poc")
            //.enableHiveSupport()
            .getOrCreate();

        // We can experiment with this to make the batch run faster
        // session.conf().set("spark.sql.shuffle.partitions", "10");

        Encoder<PersonInfo> schema = Encoders.bean(PersonInfo.class);


        Dataset<Row> df = session.readStream()
            //.option("badRecordsPath", "/Users/staleti/hadoop/badRecordsPath")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema.schema())
            .json("/Users/staleti/Walmart-Projects/data/stream-poc-1")
            .coalesce(4)
            .toDF();

        //df.printSchema();
        //System.out.println(Arrays.asList(df.columns()));
        //df.show();

        //long corruptRecordCount = df.filter("_corrupt_record").count();
        //System.out.println("corruptRecordCount :" + corruptRecordCount);

        // df.dropDuplicates()
        //df.show();

        // start some dataframe operations
        df.createOrReplaceTempView("poc_json_view");

        // key, value, timestamp
        /*Dataset<Row> invalidMessages =
            session.sql("select _corrupt_record from poc_json_view");
        StreamingQuery invalidQ = invalidMessages
            .writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            .start();*/

        // key, value, timestamp
    /*    Dataset<Row> allMessages =
            session.sql("select * from poc_json_view");
        StreamingQuery allQ = allMessages
            .writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            .start();*/

        /*System.out.println( "_corrupt_record: " + df.filter(col("_corrupt_record").isNotNull()).count());*/

        Dataset<Row> corruptedMessages =
            session.sql("select * from poc_json_view").filter(col("_corrupt_record").isNotNull());

        StreamingQuery corruptedMessagesQuery = corruptedMessages
            .writeStream()
            .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                @Override
                public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                    if (!rowDataset.isEmpty()) {
                        spotlightRequest.setBody(rowDataset.collectAsList().toString()).execute();// Async call
                        rowDataset.write().save("/Users/staleti/Walmart-Projects/data/corrupted");
                            //.json("");
                    }
                }
            })
            .start();

        Dataset<Row> validMessages =
            session.sql("select * from poc_json_view").filter(col("_corrupt_record").isNull());

        StreamingQuery query = validMessages
            .writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            .start();

       /*StreamingQuery query = validMessages
            .writeStream()
            .format("parquet")
            .partitionBy("dob")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", "/Users/staleti/hadoop")
            .option("path", "/Users/staleti/hadoop/data")
            //.option("truncate", false)
            //.option("numRows", 50)
            .start();*/

        //invalidQ.awaitTermination();
        //allQ.awaitTermination();
        corruptedMessagesQuery.awaitTermination();
        query.awaitTermination();
    }
}
