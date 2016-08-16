package org.vincentg.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class MQILineRecordReader extends RecordReader<String, String> {

	long pos;
	long start;
	long end;

	private int maxLineLength;

	private FSDataInputStream fileIn;
	private LineReader lreader = null;
	private Text valTmp = new Text();

	String key;
	String value;

	@Override
	public void initialize(InputSplit genSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		FileSplit split = (FileSplit) genSplit;

		Configuration conf = context.getConfiguration();

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(conf);

		fileIn = fs.open(file);

		start = split.getStart();
		end = start + split.getLength();

		fileIn.seek(start);
		lreader = new LineReader(fileIn);

		maxLineLength = conf.getInt(
				"mapreduce.input.linerecordreader.line.maxlength",
				Integer.MAX_VALUE);

		pos = start;

		key = file.getName();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		boolean hasNext = true;

		long bytesRead = 0;

		bytesRead = lreader.readLine(valTmp, maxLineLength, Math.max(
				maxBytesToConsume(pos), maxLineLength));

		pos += bytesRead;

		value = valTmp.toString();

		if (bytesRead == 0) {
			hasNext = false;
		}

		return hasNext;
	}

	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public String getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		try {
			if (lreader != null) {
				lreader.close();
			}
		} finally {
			fileIn.close();
		}
	}

	private int maxBytesToConsume(long pos) {
		return (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

}
