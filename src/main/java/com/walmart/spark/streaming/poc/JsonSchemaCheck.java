package com.walmart.spark.streaming.poc;

import com.walmart.spark.streaming.poc.domain.PersonInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;


public class JsonSchemaCheck {

    public static final AsyncHttpClient client = Dsl.asyncHttpClient();

    public static void main(String[] args) throws StreamingQueryException {

        BoundRequestBuilder spotlightRequest = client.prepareGet("http://localhost:8080/post");

        System.setProperty("hadoop.home.dir", "/Users/staleti/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("spark-streaming-poc")
            .getOrCreate();

        StructType jsonSchema = new StructType()
            .add("name", "string")
            .add("job", "string ")
            .add("city", "string ")
            .add("state", "string ")
            .add("dob", "string ");

        StructType schema = session.read()
            .json("/Users/staleti/Walmart-Projects/data/stream-poc/json1.json")
            .schema();


        schema.printTreeString();

        Encoder<PersonInfo> sampleEncoder = Encoders.bean(PersonInfo.class);
        System.out.println("---------------");
        sampleEncoder.schema().printTreeString();

        spotlightRequest.execute();
    }
}
