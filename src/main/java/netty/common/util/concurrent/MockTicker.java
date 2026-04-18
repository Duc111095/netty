package netty.common.util.concurrent;

import java.util.concurrent.TimeUnit;

public interface MockTicker extends Ticker {
	
	@Override
	default long initialNanoTime() {
		return 0;
	}
	
	void advance(long amount, TimeUnit unit);
	
	default void advanceMillis(long amountMillis) {
		advance(amountMillis, TimeUnit.MILLISECONDS);
	}
}
