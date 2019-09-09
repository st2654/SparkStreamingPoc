package com.walmart.spark.streaming.poc.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class SourceGenerator {
    public static void main(String[] args) {
        String directory = "/Users/staleti/Walmart-Projects/data/stream-poc-1";

        long i = 1;
        while (i <= 4) {
            i++;
            String json = null;
            if (i % 4 == 0) {
                json = "{\"name\":\"Sam\",\"job\":\"Software Engineer\",\"city\":\"Reston\",\"state\":\"VA\",\"dob\":\"20190808\","
                    + "\"appointments\":[{\"name\":\"Mazda\"},{\"name\":\"Office\"},{\"name\":\"Plumber\"}]}";

            } else if (i % 4 == 1) {
                json = "{\"name\":\"Tom\",\"job\":\"Plumber\",\"city\":\"Ashburn\",\"state\":\"VA\",\"dob\":\"20190807\"}";
            } else if (i % 4 == 2) {
                json = "{\"name\":\"Harry\",\"job\":\"Realtor\",\"city\":\"DC\",\"state\":\"DC\",\"dob\":\"20180702\","
                    + "\"appointments\":[{\"name\":\"Nissan\"},{\"name\":\"Bank\"},{\"name\":\"Poker\"}]}";
            } else {
                // TODO : change nme to name
                json = "{\"nme\":\"Yon\",\"job\":\"Pilot\",\"city\":\"San Fransisco\",\"state\":\"CA\",\"dob\":\"20190808\"}";
            }

            String fileName = UUID.randomUUID() + ".json";
            Path path = Paths.get(directory, fileName);
            try {
                Files.write(path, json.getBytes(), StandardOpenOption.CREATE);
                System.out.println("File Created");
            } catch (IOException e) {
                e.printStackTrace();
                // exception handling
            }


        }// while loop ends here


    }
}
