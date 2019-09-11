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
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;

import static org.apache.spark.sql.functions.col;

/**
 * Json Mode : FAILFAST works only if the json file is corrupted.
 * FailFast does not work if the fields are missing. Let me try with making the fields nullable.
 * JsonMode: PERMISSIVE
 *
 * https://docs.azuredatabricks.net/_static/notebooks/read-csv-corrupt-record.html
 */
public class InvalidMessagePoc {
    public static final AsyncHttpClient client = Dsl.asyncHttpClient();

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "/Users/staleti/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        BoundRequestBuilder spotlightRequest = client.preparePost("http://localhost:8080/post");

        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("spark-streaming-poc")
            .getOrCreate();

        Encoder<PersonInfo> schema = Encoders.bean(PersonInfo.class);


        Dataset<Row> df = session.readStream()
            // badRecordsPath does not work
            //.option("badRecordsPath", "/Users/staleti/hadoop/badRecordsPath")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema.schema())
            .json("/Users/staleti/Walmart-Projects/data/stream-poc-1")
            .toDF();

        // start some dataframe operations
        df.createOrReplaceTempView("poc_json_view");

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
                        // need to find out option to use the same path for saving again and again.
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

        corruptedMessagesQuery.awaitTermination();
        query.awaitTermination();
    }
}
