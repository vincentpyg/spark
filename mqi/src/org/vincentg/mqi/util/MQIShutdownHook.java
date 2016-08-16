package org.vincentg.mqi.util;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MQIShutdownHook extends Thread {

	JavaStreamingContext jsctx;

	public MQIShutdownHook(JavaStreamingContext jsctx) {
		this.jsctx = jsctx;
	}

	@Override
	public void run() {
		System.out.println("Initiating Graceful Shutdown");
		jsctx.stop(true, true);
		System.out.println("Graceful Shutdown Complete");
	}

}
