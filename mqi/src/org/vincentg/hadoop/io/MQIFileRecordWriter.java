package org.vincentg.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/***
 *
 * @author vincentg
 * @since 03-28-2016
 *
 */
public class MQIFileRecordWriter<K,V> extends RecordWriter<K,V> {

	DataOutputStream out;

	public MQIFileRecordWriter(DataOutputStream out) {
		this.out = out;
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		try {
			String tval = value.toString();
			out.write(tval.getBytes());
			out.writeBytes("\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}
}
