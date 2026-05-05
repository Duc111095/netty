package netty.buffer;

import netty.common.util.ResourceLeakDetector;
import netty.common.util.ResourceLeakTracker;
import netty.common.util.internal.MathUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;

import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;

public abstract class AbstractByteBufAllocator implements ByteBufAllocator{
	static final int DEFAULT_INITIAL_CAPACITY = 256;
	static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
	static final int DEFAULT_MAX_COMPONENTS = 16;
	static final int CALCULATE_THRESHOLD = 1048576 * 4;
	
	static {
		ResourceLeakDetector.addExclusions(AbstractByteBufAllocator.class, "toLeakAwareBuffer");
	}
	
	protected static ByteBuf toLeakAwareBuffer(ByteBuf buf) {
		ResourceLeakTracker<ByteBuf> leak = AbstractByteBuf.leakDetector.track(buf);
		if (leak != null) {
			if (AbstractByteBuf.leakDetector.isRecordEnabled()) {
				buf = new AdvancedLeakAwareByteBuf(buf, leak);
			} else {
				buf = new SimpleLeakAwareByteBuf(buf, leak);
			}
		}
		return buf;
	}
	
	protected static CompositeByteBuf toLeakAwareBuffer(CompositeByteBuf buf) {
		ResourceLeakTracker<ByteBuf> leak = AbstractByteBuf.leakDetector.track(buf);
		if (leak != null) {
			if (AbstractByteBuf.leakDetector.isRecordEnabled()) {
				buf = new AdvancedLeakAwareCompositeByteBuf(buf, leak);
			} else {
				buf = new SimpleLeakAwareCompositeByteBuf(buf, leak);
			}	
		}	
		return buf;
	}
	
	private final boolean directByDefault;
	private final ByteBuf emptyBuf;
	
	protected AbstractByteBufAllocator() {
		this(false);
	}
	
	protected AbstractByteBufAllocator(boolean preferDirect) {
		directByDefault = preferDirect && PlatformDependent.canReliabilyFreeDirectBuffer();
		emptyBuf = new EmptyByteBuf(this);
	}
	
	@Override
	public ByteBuf buffer() {
		if (directByDefault) {
			return directBuffer();
		}
		return heapBuffer();
	}
	
	@Override
	public ByteBuf buffer(int initialCapacity) {
		if (directByDefault) {
			return directBuffer(initialCapacity);
		}
		return heapBuffer(initialCapacity);
	}
	
	@Override
	public ByteBuf buffer(int initialCapacity, int maxCapacity) {
		if (directByDefault) {
			return directBuffer(initialCapacity, maxCapacity);
		}
		return heapBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
	public ByteBuf ioBuffer() {
		if (PlatformDependent.canReliabilyFreeDirectBuffer() || isDirectBufferPooled()) {
			return directBuffer(DEFAULT_INITIAL_CAPACITY);
		}
		return heapBuffer(DEFAULT_INITIAL_CAPACITY);
	}
	
	@Override
	public ByteBuf ioBuffer(int initialCapacity) {
		if (PlatformDependent.canReliabilyFreeDirectBuffer() || isDirectBufferPooled()) {
			return directBuffer(initialCapacity);
		}
		return heapBuffer(initialCapacity);
	}
	
	@Override
	public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
		if (PlatformDependent.canReliabilyFreeDirectBuffer() || isDirectBufferPooled()) {
			return directBuffer(initialCapacity, maxCapacity);
		}
		return heapBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
	public ByteBuf heapBuffer() {
		return heapBuffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
	}
	
	@Override
	public ByteBuf heapBuffer(int initialCapacity) {
		return heapBuffer(initialCapacity, DEFAULT_MAX_CAPACITY);
	}
	
	@Override
	public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
		if (initialCapacity == 0 && maxCapacity == 0) {
			return emptyBuf;
		}
		validate(initialCapacity, maxCapacity);
		return newHeapBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
	public ByteBuf directBuffer() {
		return directBuffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
	}
	
	@Override
	public ByteBuf directBuffer(int initialCapacity) {
		return directBuffer(initialCapacity, DEFAULT_MAX_CAPACITY);
	}
	
	@Override
	public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
		if (initialCapacity == 0 && maxCapacity == 0) {
			return emptyBuf;
		}
		validate(initialCapacity, maxCapacity);
		return newDirectBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
	public CompositeByteBuf compositeBuffer() {
		if (directByDefault) {
			return compositeDirectBuffer();
		}
		return compositeHeapBuffer();
	}
	
	@Override
	public CompositeByteBuf compositeBuffer(int maxNumComponents) {
		if (directByDefault) {
			return compositeDirectBuffer(maxNumComponents);
		}
		return compositeHeapBuffer(maxNumComponents);
	}
	
	@Override
	public CompositeByteBuf compositeHeapBuffer() {
		return compositeHeapBuffer(DEFAULT_MAX_COMPONENTS);
	}
	
	@Override
	public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
		return toLeakAwareBuffer(new CompositeByteBuf(this, false, maxNumComponents));
	}
	
	@Override
	public CompositeByteBuf compositeDirectBuffer() {
		return compositeDirectBuffer(DEFAULT_MAX_COMPONENTS);
	}
	
	@Override
	public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
		return toLeakAwareBuffer(new CompositeByteBuf(this, true, maxNumComponents));
	}
	
	private static void validate(int initialCapacity, int maxCapacity) {
		checkPositiveOrZero(initialCapacity, "initialCapacity");
		if (initialCapacity > maxCapacity) {
			throw new IllegalArgumentException(String.format(
					"initialCapacity: %d (expected: not greater than maxCapacity(%d)", 
					initialCapacity, maxCapacity));
		}
	}
	
	protected abstract ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity);
	
	protected abstract ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity);
	
	@Override
	public String toString() {
		return StringUtil.simpleClassName(this) + "(directByDefault: " + directByDefault + ')';
	}
	
	@Override
	public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
		checkPositiveOrZero(minNewCapacity, "minNewCapacity");
		if (minNewCapacity > maxCapacity) {
			throw new IllegalArgumentException(String.format(
					"initialCapacity: %d (expected: not greater than maxCapacity(%d)", 
					minNewCapacity, maxCapacity));
		}
		final int threshold = CALCULATE_THRESHOLD;
		
		if (minNewCapacity == threshold) {
			return threshold;
		}
		
		if (minNewCapacity > threshold) {
			int newCapacity = minNewCapacity / threshold * threshold;
			if (newCapacity > maxCapacity - threshold) {
				newCapacity = maxCapacity;
			} else {
				newCapacity += threshold;
			}
			return newCapacity;
		}
		final int newCapacity = MathUtil.findNextPositivePowerOfTwo(Math.max(minNewCapacity, 64));
		return Math.min(newCapacity, maxCapacity);
	}
}
