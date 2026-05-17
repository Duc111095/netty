package netty.channel;

import netty.common.util.internal.AdaptiveCalculator;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public class AdaptiveRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {
	
	public static final int DEFAULT_MINIMUM = 64;
	public static final int DEFAULT_INITIAL = 2048;
	public static final int DEFAULT_MAXIMUM = 65536;
	
	public static final AdaptiveRecvByteBufAllocator DEFAULT = new AdaptiveRecvByteBufAllocator();
	
	private final class HandleImpl extends MaxMessageHandle {
		private final AdaptiveCalculator calculator;
		
		HandleImpl(int minimum, int initial, int maximum) {
			calculator = new AdaptiveCalculator(minimum, initial, maximum);
		}
		
		@Override
		public void lastBytesRead(int bytes) {
			if (bytes == attemptedBytesRead()) {
				calculator.record(bytes);
			}
			super.lastBytesRead(bytes);
		}
		
		@Override
		public int guess() {
			return calculator.nextSize();
		}
		
		@Override
		public void readComplete() {
			calculator.record(totalBytesRead());
		}
	}
	
	private final int minimum;
	private final int initial;
	private final int maximum;
	
	public AdaptiveRecvByteBufAllocator() {
		this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
	}
	
	public AdaptiveRecvByteBufAllocator(int minimum, int initial, int maximum) {
		checkPositive(minimum, "minimum");
		if (initial < minimum) {
			throw new IllegalArgumentException("initial: " + initial);
		}
		if (maximum < initial) {
			throw new IllegalArgumentException("maximum: " + maximum);
		}
		
		this.minimum = minimum;
		this.maximum = maximum;
		this.initial = initial;
	}
	
	@Override
	public Handle newHandle() {
		return new HandleImpl(minimum, initial, maximum);
	}
	
	@Override
    public AdaptiveRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        super.respectMaybeMoreData(respectMaybeMoreData);
        return this;
    }
}
