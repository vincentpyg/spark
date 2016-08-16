package org.vincentg.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/***
*
* @author vincentg
* @since 03-28-2016
*
*/
public class MQIBatchFileOutputFormat<K,V> extends TextOutputFormat<K,V>{

	@Override
	public RecordWriter<K,V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		DataOutputStream out;
		Configuration conf = job.getConfiguration();

		Path file = getDefaultWorkFile(job, "");
		Date d = new Date();
		String npath = file.toString()+""+d.getTime();

		file = new Path(npath);
		FileSystem fs = file.getFileSystem(conf);
		out = fs.create(file, false);

		return new MQIFileRecordWriter<K,V>(out);

	}

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		//stub: bypass output directory checking
	}


}
