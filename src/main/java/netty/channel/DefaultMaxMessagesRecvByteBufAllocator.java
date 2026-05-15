package netty.channel;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.common.util.UncheckedBooleanSupplier;
import netty.common.util.internal.ObjectUtil;

public abstract class DefaultMaxMessagesRecvByteBufAllocator implements MaxMessagesRecvByteBufAllocator {
	private final boolean ignoreBytesRead;
	private volatile int maxMessagesPerRead;
	private volatile boolean respectMaybeMoreData = true;
	
	public DefaultMaxMessagesRecvByteBufAllocator() {
		this(1);
	}
	
	public DefaultMaxMessagesRecvByteBufAllocator(int maxMessagesPerRead) {
		this(maxMessagesPerRead, false);
	}
	
	DefaultMaxMessagesRecvByteBufAllocator(int maxMessagesPerRead, boolean ignoreBytesRead) {
		this.ignoreBytesRead = ignoreBytesRead;
		maxMessagesPerRead(maxMessagesPerRead);
	}
	
	@Override
	public int maxMessagesPerRead() {
		return maxMessagesPerRead;
	}
	
	@Override
	public MaxMessagesRecvByteBufAllocator maxMessagesPerRead(int maxMessagesPerRead) {
		ObjectUtil.checkPositive(maxMessagesPerRead, "maxMessagesPerRead");
		this.maxMessagesPerRead = maxMessagesPerRead;
		return this;
	}
	
	public DefaultMaxMessagesRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
		this.respectMaybeMoreData = respectMaybeMoreData;
		return this;
	}
	
	public final boolean respectMaybeMoreData() {
		return respectMaybeMoreData;
	}
	
	public abstract class MaxMessageHandle implements ExtendedHandle {
		private ChannelConfig config;
		private int maxMessagesPerRead;
		private int totalMessages;
		private int totalBytesRead;
		private int attemptedBytesRead;
		private int lastBytesRead;
		private final boolean respectMaybeMoreData = DefaultMaxMessagesRecvByteBufAllocator.this.respectMaybeMoreData;
		private final UncheckedBooleanSupplier defaultMaybeMoreSupplier = new UncheckedBooleanSupplier() {
			@Override
			public boolean get() {
				return attemptedBytesRead == lastBytesRead;
			}
		};
		
		@Override
		public void reset(ChannelConfig config) {
			this.config = config;
			maxMessagesPerRead = maxMessagesPerRead();
			totalMessages = totalBytesRead = 0;
		}
		
		@Override
		public ByteBuf allocate(ByteBufAllocator alloc) {
			return alloc.ioBuffer(guess());
		}
		
		@Override
		public final void incMessagesRead(int amt) {
			totalMessages += amt;
		}
		
		@Override
		public void lastBytesRead(int bytes) {
			lastBytesRead = bytes;
			if (bytes > 0) {
				totalBytesRead += bytes;
			}
		}
		
		@Override
		public final int lastBytesRead() {
			return lastBytesRead;
		}
		
		@Override
		public boolean continueReading() {
			return continueReading(defaultMaybeMoreSupplier);
		}
		
		@Override
		public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
			return config.isAutoRead() &&
					(!respectMaybeMoreData || maybeMoreDataSupplier.get()) &&
					totalMessages < maxMessagesPerRead && (ignoreBytesRead || totalBytesRead > 0);
		}
		
		@Override
		public void readComplete() {
			
		}
		
		@Override
		public int attemptedBytesRead() {
			return attemptedBytesRead;
		}
		
		@Override
		public void attemptedBytesRead(int bytes) {
			attemptedBytesRead = bytes;
		}
		
		protected final int totalBytesRead() {
			return totalBytesRead < 0 ? Integer.MAX_VALUE : totalBytesRead;
		}
	}
}
