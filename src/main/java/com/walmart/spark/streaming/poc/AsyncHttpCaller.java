package com.walmart.spark.streaming.poc;

import com.walmart.spark.streaming.poc.http.Request;
import com.walmart.spark.streaming.poc.http.Response;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncHttpCaller {

    public static void main(String[] args) throws InterruptedException, ExecutionException, MalformedURLException {
        AsyncHttpCaller.call();
    }
    public static void call() throws ExecutionException, InterruptedException, MalformedURLException {
        // Have one (or more) threads ready to do the async tasks. Do this during startup of your app.
        ExecutorService executor = Executors.newFixedThreadPool(1);

        // Fire a request.
        Future<Response> response = executor.submit(new Request(new URL("http://localhost:8080/post")));

        // Do your other tasks here (will be processed immediately, current thread won't block).
        // ...

        // Get the response (here the current thread will block until response is returned).
        InputStream body = response.get().getBody();
        // ...

        // Shutdown the threads during shutdown of your app.
        executor.shutdown();
    }

}

