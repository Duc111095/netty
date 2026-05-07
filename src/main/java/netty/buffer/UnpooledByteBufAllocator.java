package netty.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;

import netty.common.util.internal.CleanableDirectBuffer;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;

public final class UnpooledByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {
	
	private final UnpooledByteBufAllocatorMetric metric = new UnpooledByteBufAllocatorMetric();
	private final boolean disableLeakDetector;
	private final boolean noCleaner;
	
	public static final UnpooledByteBufAllocator DEFAULT =
			new UnpooledByteBufAllocator(PlatformDependent.directBufferPreferred());
	
	public UnpooledByteBufAllocator(boolean preferDirect) {
		this(preferDirect, false);
	}
	
	public UnpooledByteBufAllocator(boolean preferDirect, boolean disableLeakDetector) {
		this(preferDirect, disableLeakDetector, PlatformDependent.useDirectBufferNoCleaner());
	}
	
	public UnpooledByteBufAllocator(boolean preferDirect, boolean disableLeakDetector, boolean tryNoCleaner) {
		super(preferDirect);
		this.disableLeakDetector = disableLeakDetector;
		noCleaner = tryNoCleaner && PlatformDependent.hasUnsafe() 
				&& PlatformDependent.hasDirectBufferNoCleanerConstructor();
	}
	
	@Override
	protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
		return PlatformDependent.hasUnsafe() ?
				new InstrumentedUnpooledUnsafeHeapByteBuf(this, initialCapacity, maxCapacity) :
				new InstrumentedUnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
	}
	
	@Override
	protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
		final ByteBuf buf;
		if (PlatformDependent.hasUnsafe()) {
			buf = noCleaner ? new InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf(this, initialCapacity, maxCapacity) :
				new InstrumentedUnpooledUnsafeDirectByteBuf(this, initialCapacity, maxCapacity);
		} else {
			buf = new InstrumentedUnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
		}
		return disableLeakDetector ? buf : toLeakAwareBuffer(buf);
	}
	
	@Override
	public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
		CompositeByteBuf buf = new CompositeByteBuf(this, false, maxNumComponents);
		return disableLeakDetector ? buf : toLeakAwareBuffer(buf);
	}
	
	@Override
	public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
		CompositeByteBuf buf = new CompositeByteBuf(this, true, maxNumComponents);
		return disableLeakDetector ? buf : toLeakAwareBuffer(buf);
	}
	
	@Override
	public boolean isDirectBufferPooled() {
		return false;
	}
	
	@Override
	public ByteBufAllocatorMetric metric() {
		return metric;
	}
	
	void incrementDirect(int amount) {
		metric.directCounter.add(amount);
	}
	
	void decrementDirect(int amount) {
		metric.directCounter.add(-amount);
	}
	
	void incrementHeap(int amount) {
		metric.heapCounter.add(amount);
	}
	
	void decrementHeap(int amount) {
		metric.heapCounter.add(-amount);
	}
	
	private static final class InstrumentedUnpooledUnsafeHeapByteBuf extends UnpooledUnsafeHeapBuffer {
		InstrumentedUnpooledUnsafeHeapByteBuf(UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
			super(alloc, initialCapacity, maxCapacity);	
		}
		
		@Override
		protected byte[] allocateArray(int initialCapacity) {
			byte[] bytes = super.allocateArray(initialCapacity);
			((UnpooledByteBufAllocator) alloc()).incrementHeap(bytes.length);
			return bytes;
		}
		
		@Override
		protected void freeArray(byte[] array) {
			int length = array.length;
			super.freeArray(array);
			((UnpooledByteBufAllocator) alloc()).decrementHeap(length);
		}
	}
	
	private static final class InstrumentedUnpooledHeapByteBuf extends UnpooledHeapByteBuf {
		InstrumentedUnpooledHeapByteBuf(UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
			super(alloc, initialCapacity, maxCapacity);
		}
		
		@Override
		protected byte[] allocateArray(int initialCapacity) {
			byte[] bytes = super.allocateArray(initialCapacity);
			((UnpooledByteBufAllocator) alloc()).incrementHeap(bytes.length);
			return bytes;
		}
		
		@Override
		protected void freeArray(byte[] array) {
			int length = array.length;
			super.freeArray(array);
			((UnpooledByteBufAllocator) alloc()).decrementHeap(length);
		}
	}
	
	private static final class InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf
			extends UnpooledUnsafeNoCleanerDirectByteBuf {
		InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf(
				UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
			super(alloc, initialCapacity, maxCapacity);
		}
		
		@Override
		protected CleanableDirectBuffer allocateDirectBuffer(int capacity) {
			CleanableDirectBuffer buffer = super.allocateDirectBuffer(capacity);
			return new DecrementingCleanableDirectBuffer(alloc(), buffer);
		}
		
		@Override
		protected CleanableDirectBuffer allocateDirectBuffer(int capacity, boolean permitExpensiveClean) {
			CleanableDirectBuffer buffer = super.allocateDirectBuffer(capacity, permitExpensiveClean);
			return new DecrementingCleanableDirectBuffer(alloc(), buffer);
		}
		
		@Override
		CleanableDirectBuffer reallocateDirect(CleanableDirectBuffer oldBuffer, int newCapacity) {
			int oldCapacity = oldBuffer.buffer().capacity();
			CleanableDirectBuffer buffer = super.reallocateDirect(oldBuffer, newCapacity);
			return new DecrementingCleanableDirectBuffer(
					alloc(), buffer, buffer.buffer().capacity() - oldCapacity);
		}
	}
	
	private static final class InstrumentedUnpooledUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
		InstrumentedUnpooledUnsafeDirectByteBuf(
				UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
			super(alloc, initialCapacity, maxCapacity);
		}
		
		@Override
		protected CleanableDirectBuffer allocateDirectBuffer(int capacity) {
			CleanableDirectBuffer buffer = super.allocateDirectBuffer(capacity);
			return new DecrementingCleanableDirectBuffer(alloc(), buffer);
		}
		
		@Override
		protected CleanableDirectBuffer allocateDirectBuffer(int capacity, boolean permitExpensiveClean) {
			CleanableDirectBuffer buffer = super.allocateDirectBuffer(capacity, permitExpensiveClean);
			return new DecrementingCleanableDirectBuffer(alloc(), buffer);
		}
		
		@Override
		protected ByteBuf allocateDirect(int initialCapacity) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		protected void freeDirect(ByteBuffer buffer) {
			throw new UnsupportedOperationException();
		}
	}
	
	private static final class InstrumentedUnpooledDirectByteBuf extends UnpooledDirectByteBuf {
        InstrumentedUnpooledDirectByteBuf(
                UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
            super(alloc, initialCapacity, maxCapacity);
        }

        @Override
        protected CleanableDirectBuffer allocateDirectBuffer(int initialCapacity) {
            CleanableDirectBuffer buffer = super.allocateDirectBuffer(initialCapacity);
            return new DecrementingCleanableDirectBuffer(alloc(), buffer);
        }

        @Override
        protected CleanableDirectBuffer allocateDirectBuffer(int initialCapacity, boolean permitExpensiveClean) {
            CleanableDirectBuffer buffer = super.allocateDirectBuffer(initialCapacity, permitExpensiveClean);
            return new DecrementingCleanableDirectBuffer(alloc(), buffer);
        }

        @Override
        protected ByteBuffer allocateDirect(int initialCapacity) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void freeDirect(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }
    }
	
	private static final class DecrementingCleanableDirectBuffer implements CleanableDirectBuffer {
		private final UnpooledByteBufAllocator alloc;
		private final CleanableDirectBuffer  delegate;
		
		private DecrementingCleanableDirectBuffer(
				ByteBufAllocator alloc, CleanableDirectBuffer delegate) {
			this(alloc, delegate, delegate.buffer().capacity());
		}
		
		private DecrementingCleanableDirectBuffer(
				ByteBufAllocator alloc, CleanableDirectBuffer delegate, int capacityConsumed) {
			this.alloc = (UnpooledByteBufAllocator) alloc;
			this.alloc.incrementDirect(capacityConsumed);
			this.delegate = delegate;
		}
		
		@Override
		public ByteBuffer buffer() {
			return delegate.buffer();
		}
		
		@Override
		public void clean() {
			int capacity = delegate.buffer().capacity();
			delegate.clean();
			alloc.decrementDirect(capacity);
		}
		
		@Override
		public boolean hasMemoryAddress() {
			return delegate.hasMemoryAddress();
		}
		
		@Override
		public long memoryAddress() {
			return delegate.memoryAddress();
		}
	} 
	
	private static final class UnpooledByteBufAllocatorMetric implements ByteBufAllocatorMetric {
		final LongAdder directCounter = new LongAdder();
		final LongAdder heapCounter = new LongAdder();
		
		@Override
		public long usedHeapMemory() {
			return heapCounter.sum();
		}
		
		@Override
		public long usedDirectMemory() {
			return directCounter.sum();
		}
		
		@Override
		public String toString() {
			return StringUtil.simpleClassName(this) +
					"(usedHeapMemory: " + usedHeapMemory() + "; usedDirectMemory: " + usedDirectMemory() + ')';
		}
	}
}	
