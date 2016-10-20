package spark.sample;

import hadoop.io.CustomTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Contains implementation for stream processing and stream recovery
 * Created by vincentg on 19/10/16.
 */
public class StreamingContextFunction implements Function0<JavaStreamingContext> {

    @Override
    public JavaStreamingContext call() throws Exception {

        final SparkConf conf = new SparkConf().setMaster("local").setAppName("app1");

        //STREAMING
        conf.set("spark.streaming.stopGracefullyOnShutdown","true"); // graceful shutdown of spark app
        conf.set("spark.streaming.fileStream.minRememberDuration", "31557600s"); // read from current time - 1yr
        JavaStreamingContext sctx = new JavaStreamingContext(conf, Durations.seconds(10));
        sctx.checkpoint("./__checkpoint/"); //for recovery during streaming

        JavaPairInputDStream<String, String> streamRdd = sctx.fileStream(
                "/Users/diamonduser/Desktop/sample",
                String.class,
                String.class,
                CustomTextInputFormat.class,
                (Path p) -> (p.getName().contains("COPYING") || p.getName().contains("DS") ) ?
                        Boolean.FALSE : Boolean.TRUE,
                false);
        streamRdd.checkpoint(Durations.seconds(10));
        streamRdd.map( t -> t._2()).print(); //print what is in the data stream

        return sctx;
    }
}
