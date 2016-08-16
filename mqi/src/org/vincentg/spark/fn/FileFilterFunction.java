package org.vincentg.spark.fn;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;

public class FileFilterFunction implements Function<Path, Boolean>{

	private static final long serialVersionUID = -8424583698569941662L;

	@Override
	public Boolean call(Path t) throws Exception {
		if (t.getName().contains("COPYING")) {
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}
}
