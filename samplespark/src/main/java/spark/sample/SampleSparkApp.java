package spark.sample;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * Sample Spark App used as reference for mixed batch and streaming (with checkpoints)
 * Created by vincentg on 18/10/16.
 */
public class SampleSparkApp implements Serializable {

    public SampleSparkApp() {

        try {

            JavaStreamingContext sctx = JavaStreamingContext.getOrCreate(
                    "./__checkpoint",
                    new StreamingContextFunction()); // stream processing

            //batch processing
            JavaRDD<String> batchRDD = sctx.sparkContext().textFile("./sample");
                        batchRDD.foreach((s) -> System.out.println("batch => "+s));

            sctx.start();
            sctx.awaitTermination();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }


//        Batch processing using SparkSQL

        //INSTANTIATE CONTEXT (session)
//        SparkSession session = SparkSession
//                .builder()
//                .master("local")
//                .appName("checkpoint")
//                .getOrCreate();

        //READ FROM SOURCE
//        session.read().jdbc() //read from JDBC data store
//        Dataset<Row> rows = session.read().text("./samplesample.txt"); //read from Text File

//        rows.foreach( (Row row) -> System.out.println(row.getString(0)) );

    }
}
