package netty.common.util.concurrent;

import java.util.concurrent.TimeUnit;

public interface Ticker {
	
	static Ticker systemTicker() {
		return SystemTicker.INSTANCE;
	}
	
	static MockTicker newMockTicker() {
		return new DefaultMockTicker();
	}
	
	long initialNanoTime();
	
	long nanoTime();
	
	void sleep(long delay, TimeUnit unit) throws InterruptedException;
	
	default void sleepMillis(long delayMillis) throws InterruptedException {
		sleep(delayMillis, TimeUnit.MILLISECONDS);
	}
}
