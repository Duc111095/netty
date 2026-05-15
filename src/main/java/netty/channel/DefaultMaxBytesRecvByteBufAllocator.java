package netty.channel;

import java.util.AbstractMap;
import java.util.Map.Entry;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.common.util.UncheckedBooleanSupplier;
import netty.common.util.internal.ObjectUtil;

public class DefaultMaxBytesRecvByteBufAllocator implements MaxBytesRecvByteBufAllocator {
	private volatile int maxBytesPerRead;
	private volatile int maxBytesPerIndividualRead;
	
	private final class HandleImpl implements ExtendedHandle {
		private int individualReadMax;
		private int bytesToRead;
		private int lastBytesRead;
		private int attemptBytesRead;
		private final UncheckedBooleanSupplier defaultMaybeMoreSupplier = new UncheckedBooleanSupplier() {
			@Override
			public boolean get() {
				return attemptBytesRead == lastBytesRead;
			}
		};
		
		@Override
		public ByteBuf allocate(ByteBufAllocator alloc) {
			return alloc.ioBuffer(guess());
		}
		
		@Override
		public int guess() {
			return Math.min(individualReadMax, bytesToRead);
		}
		
		@Override
		public void reset(ChannelConfig config) {
			bytesToRead = maxBytesPerRead();
			individualReadMax = maxBytesPerIndividualRead();
		}
		
		@Override
		public void incMessagesRead(int amt) {
			
		}
		
		@Override
		public void lastBytesRead(int bytes) {
			lastBytesRead = bytes;
			bytesToRead -= bytes;
		}
		
		@Override
		public int lastBytesRead() {
			return lastBytesRead;
		}
		
		@Override
		public boolean continueReading() {
			return continueReading(defaultMaybeMoreSupplier);
		}
		
		@Override
		public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
			return bytesToRead > 0 && maybeMoreDataSupplier.get();
		}
		
		@Override
		public void readComplete() {
			
		}
		
		@Override
		public void attemptedBytesRead(int bytes) {
			attemptBytesRead = bytes;
		}
		
		@Override
		public int attemptedBytesRead() {
			return attemptBytesRead;
		}
	}
	
	public DefaultMaxBytesRecvByteBufAllocator() {
		this(64 * 1024, 64 * 1024);
	}
	
	public DefaultMaxBytesRecvByteBufAllocator(int maxBytesPerRead, int maxBytesPerIndividualRead) {
		checkMaxBytesPerReadPair(maxBytesPerRead, maxBytesPerIndividualRead);
		this.maxBytesPerRead = maxBytesPerRead;
		this.maxBytesPerIndividualRead = maxBytesPerIndividualRead;
	}
	
	@Override
	public Handle newHandle() {
		return new HandleImpl();
	}
	
	@Override
	public int maxBytesPerRead() {
		return maxBytesPerRead;
	}
	
	@Override
	public DefaultMaxBytesRecvByteBufAllocator maxBytesPerRead(int maxBytesPerRead) {
		ObjectUtil.checkPositive(maxBytesPerRead, "maxBytesPerRead");
		synchronized (this) {
			final int maxBytesPerIndividualRead = maxBytesPerIndividualRead();
			if (maxBytesPerRead < maxBytesPerIndividualRead) {
				throw new IllegalArgumentException(
						"MaxBytesPerRead cannot be less than " + 
							"maxBytesPerIndividualRead (" + maxBytesPerIndividualRead + "): " + maxBytesPerRead);
			}
			this.maxBytesPerRead = maxBytesPerRead;
		}
		return this;
	}
	
	@Override
	public int maxBytesPerIndividualRead() {
		return maxBytesPerIndividualRead;
	}
	
	@Override
	public DefaultMaxBytesRecvByteBufAllocator maxBytesPerIndividualRead(int maxBytesPerIndividualRead) {
		ObjectUtil.checkPositive(maxBytesPerIndividualRead, "maxBytesPerIndividualRead");
		synchronized (this) {
			final int maxBytesPerRead = maxBytesPerRead();
			if (maxBytesPerIndividualRead > maxBytesPerRead) {
				throw new IllegalArgumentException(
						"maxBytesPerIndividualRead cannot be greater than " + 
							"maxBytesPerRead (" + maxBytesPerRead + "): " + maxBytesPerIndividualRead);
			}
			this.maxBytesPerIndividualRead = maxBytesPerIndividualRead;
		}
		return this;
	}
	
	@Override
	public synchronized Entry<Integer, Integer> maxBytesPerReadPair() {
		return new AbstractMap.SimpleEntry<Integer, Integer>(maxBytesPerRead, maxBytesPerIndividualRead);
	}
	
	private static void checkMaxBytesPerReadPair(int maxBytesPerRead, int maxBytesPerIndividualRead) {
		ObjectUtil.checkPositive(maxBytesPerRead, "maxBytesPerRead");
		ObjectUtil.checkPositive(maxBytesPerIndividualRead, "maxBytesPerIndividualRead");
		if (maxBytesPerRead < maxBytesPerIndividualRead) {
			throw new IllegalArgumentException(
					"maxBytesPerRead cannot be less than " + 
						"maxBytesPerIndividualRead (" + maxBytesPerIndividualRead + "): " + maxBytesPerRead);
		}
	}
	
	@Override
	public DefaultMaxBytesRecvByteBufAllocator maxBytesPerReadPair(int maxBytesPerRead,
			int maxBytesReadPerIndividualRead) {
		checkMaxBytesPerReadPair(maxBytesPerRead, maxBytesPerIndividualRead);
		synchronized (this) {
			this.maxBytesPerRead = maxBytesPerRead;
			this.maxBytesPerIndividualRead = maxBytesReadPerIndividualRead;
		}
		return this;
	}
}
