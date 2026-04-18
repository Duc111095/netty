package netty.common.util.internal;

import java.util.concurrent.atomic.LongAdder;

public final class LongAddedCounter extends LongAdder implements LongCounter {

	private static final long serialVersionUID = 4707128270553625761L;

	@Override
	public long value() {
		return longValue();
	}
}
