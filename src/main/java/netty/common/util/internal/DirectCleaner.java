package netty.common.util.internal;

import java.nio.ByteBuffer;

final class DirectCleaner implements Cleaner {
	@Override
	public CleanableDirectBuffer allocate(int capacity) {
		return new CleanableDirectBufferImpl(capacity);
	}
	
	@Override
	public CleanableDirectBuffer reallocate(CleanableDirectBuffer old, int newCapacity) {
		int oldCapacity = old.buffer().capacity();
		int delta = newCapacity - oldCapacity;
		PlatformDependent.incrementMemoryCounter(delta);
		try {
			ByteBuffer newBuffer = PlatformDependent0.reallocateDirectNoCleaner(old.buffer(), newCapacity);
			return new CleanableDirectBufferImpl(newBuffer);
		} catch(Throwable e) {
			PlatformDependent.decrementMemoryCounter(delta);
			throw e;
		}
	}
	
	@Override
	public void freeDirectBuffer(ByteBuffer buffer) {
		PlatformDependent0.freeMemory(PlatformDependent0.directBufferAddress(buffer));
	}
	
	@Override
	public boolean hasExpensiveClean() {
		return false;
	}
	
	private static final class CleanableDirectBufferImpl implements CleanableDirectBuffer {
		private final ByteBuffer buffer;
		
		CleanableDirectBufferImpl(int capacity) {
			PlatformDependent.incrementMemoryCounter(capacity);
			try {
				this.buffer = PlatformDependent0.allocateDirectNoCleaner(capacity);
			} catch (Throwable e) {
				PlatformDependent.decrementMemoryCounter(capacity);
				throw e;
			}
		}
		
		CleanableDirectBufferImpl(ByteBuffer buffer) {
			this.buffer = buffer;
		}
		
		@Override
		public ByteBuffer buffer() {
			return buffer;
		}
		
		@Override
		public void clean() {
			int capacity = buffer.capacity();
			PlatformDependent0.freeMemory(PlatformDependent0.directBufferAddress(buffer));
			PlatformDependent.decrementMemoryCounter(capacity);
		}
	}
}
