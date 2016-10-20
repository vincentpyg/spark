package hadoop.io;

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

public class CustomLineRecordReader extends RecordReader<String, String> {

	private long pos;
	private long start;
	private long end;

	private int maxLineLength;

	private FSDataInputStream fileIn;
	private LineReader lReader = null;
	private Text valTmp = new Text();

	private String key;
	private String value;

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
		lReader = new LineReader(fileIn);

		maxLineLength = conf.getInt(
				"mapreduce.input.linerecordreader.line.maxlength",
				Integer.MAX_VALUE);

		pos = start;

		key = file.getName();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		boolean hasNext = true;

		long bytesRead = lReader.readLine(valTmp, maxLineLength, Math.max(
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
			if (lReader != null) {
				lReader.close();
			}
		} finally {
			fileIn.close();
		}
	}

	private int maxBytesToConsume(long pos) {
		return (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

}
