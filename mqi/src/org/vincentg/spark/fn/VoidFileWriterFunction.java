package org.vincentg.spark.fn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.vincentg.hadoop.io.MQIStreamFileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/***
 *
 * @author vincentg
 * @since 03-28-2016
 *
 */
public class VoidFileWriterFunction implements
		VoidFunction2<JavaPairRDD<String, String>, Time> {

	private static final long serialVersionUID = 1L;

	private String outputLoc;

	public VoidFileWriterFunction(String outputLoc) {
		this.outputLoc = outputLoc;
	}

	@Override
	public void call(JavaPairRDD<String, String> v1, Time v2)
			throws Exception {

		if (!v1.isEmpty()) {
			v1.saveAsNewAPIHadoopFile(
					this.outputLoc,
					Text.class,
					Text.class,
					MQIStreamFileOutputFormat.class);
		}

	}
}
