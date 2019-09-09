package com.walmart.spark.streaming.poc;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

public class CommentedCodeClass {
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

   /* StructType jsonSchema = new StructType()
        .add("name", "string")
        .add("job", "string ")
        .add("city", "string ")
        .add("state", "string ")
        .add("dob", "string ")
        .add("appointments", new ArrayType());*/

     /*  Dataset<Row> df = session.readStream()
            .schema(jsonSchema)
            .format("json")
            .option("path", "/Users/staleti/Walmart-Projects/data/stream-poc")
            .load();*/



/*      StreamingQuery query = results
            .writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            //.option("checkpointLocation", "/Users/staleti/hadoop")
            //.option("truncate", false)
            //.option("numRows", 50)
            .start();*/

/* Dataset<Row> parquetDf = session.read()
            .schema(schema.schema())
            .parquet("/Users/staleti/hadoop/data");

        parquetDf.show();*/
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

}
