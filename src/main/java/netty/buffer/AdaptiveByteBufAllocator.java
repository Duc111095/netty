package netty.buffer;

import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class AdaptiveByteBufAllocator extends AbstractByteBufAllocator 
	implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptiveByteBufAllocator.class);
	private static final boolean DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS;
	
	static {
		DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS = SystemPropertyUtil.getBoolean(
				"io.netty.allocator.useCachedMagazinesForNonEventLoopThreads", false);
		logger.debug("-Dio.netty.allocator.useCachedMagazinesForNonEventLoopThreads: {}",
				DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
	}
	
	private final AdaptivePoolingAllocator direct;
	private final AdaptivePoolingAllocator heap;
	
	public AdaptiveByteBufAllocator() {
		this(!PlatformDependent.isExplicitNoPreferDirect());
	}
	
	public AdaptiveByteBufAllocator(boolean preferDirect) {
		this(preferDirect, DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
	}
	
	public AdaptiveByteBufAllocator(boolean preferDirect, boolean useCacheForNonEventLoopThreads) {
		super(preferDirect);
		direct = new AdaptivePoolingAllocator(new DirectChunkAllocator(this), useCacheForNonEventLoopThreads);
		heap = new AdaptivePoolingAllocator(new HeapChunkAllocator(this), useCacheForNonEventLoopThreads);
	}
	
	@Override
	protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
		return toLeakAwareBuffer(heap.allocate(initialCapacity, maxCapacity));
	}
	
	@Override
	protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
		return toLeakAwareBuffer(direct.allocate(initialCapacity, maxCapacity));
	}
	
	@Override
	public boolean isDirectBufferPooled() {
		return true;
	}
	
	@Override
	public long usedHeapMemory() {
		return heap.usedMemory();
	}
	
	@Override
	public long usedDirectMemory() {
		return direct.usedMemory();
	}
	
	@Override
	public ByteBufAllocatorMetric metric() {
		return this;
	}
	
	private static final class HeapChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
		private final ByteBufAllocator allocator;
		
		private HeapChunkAllocator(ByteBufAllocator allocator) {
			this.allocator = allocator;
		}
		
		@Override
		public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
			return PlatformDependent.hasUnsafe() ?
					new UnpooledUnsafeHeapByteBuf(allocator, initialCapacity, maxCapacity) :
					new UnpooledHeapByteBuf(allocator, initialCapacity, maxCapacity);
		}
	}
	
	private static final class DirectChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
		private final ByteBufAllocator allocator;
		
		private DirectChunkAllocator(ByteBufAllocator allocator) {
			this.allocator = allocator;
		}
		
		@Override
		public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
			return UnsafeByteBufUtil.newDirectByteBuf(allocator, initialCapacity, maxCapacity);
		}
	}
}
