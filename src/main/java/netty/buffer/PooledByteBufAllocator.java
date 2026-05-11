package netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import netty.common.util.NettyRuntime;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.concurrent.FastThreadLocalThread;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.ThreadExecutorMap;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;

public class PooledByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PooledByteBufAllocator.class);
	private static final int DEFAULT_NUM_HEAP_ARENA;
	private static final int DEFAULT_NUM_DIRECT_ARENA;
	
	private static final int DEFAULT_PAGE_SIZE;
	private static final int DEFAULT_MAX_ORDER;
	private static final int DEFAULT_SMALL_CACHE_SIZE;
	private static final int DEFAULT_NORMAL_CACHE_SIZE;
	static final int DEFAULT_MAX_CACHED_BUFFER_CAPACITY;
	private static final int DEFAULT_CACHE_TRIM_INTERVAL;
	private static final long DEFAULT_CACHE_TRIM_INTERVAL_MILLIS;
	private static final boolean DEFAULT_USE_CACHE_FOR_ALL_THREADS;
	private static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT;
	static final int DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK;
	private static final boolean DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS;
	
	private static final int MIN_PAGE_SIZE = 4096;
	private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);
	
	private static final int CACHE_NOT_USED = 0;
	
	private final Runnable trimTask = new Runnable() {
		@Override
		public void run() {
			PooledByteBufAllocator.this.trimCurrentThreadCache();
		}
	};
	
	static {
		int defaultAlignment = SystemPropertyUtil.getInt(
				"io.netty.allocator.directMemoryCacheAlignment", 0);
		int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
		Throwable pageSizeFallbackCause = null;
		try {
			validateAndCalculatePageShifts(defaultPageSize, defaultAlignment);
		} catch (Throwable t) {
			pageSizeFallbackCause =  t;
			defaultPageSize = 8192;
			defaultAlignment = 0;
		}
		DEFAULT_PAGE_SIZE = defaultPageSize;
		DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT = defaultAlignment;
		
		int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 9);
		Throwable maxOrderFallbackCause = null;
		try {
			validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
		} catch (Throwable t) {
			maxOrderFallbackCause = t;
			defaultMaxOrder = 9;
		}
		DEFAULT_MAX_ORDER = defaultMaxOrder;
		
		final Runtime runtime = Runtime.getRuntime();
		
		final int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
		final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
		DEFAULT_NUM_HEAP_ARENA = Math.max(0, 
				SystemPropertyUtil.getInt(
						"io.netty.allocator.numHeapArenas", 
						(int) Math.min(
								defaultMinNumArena, 
								runtime.maxMemory() / defaultChunkSize / 2 / 3)));
		DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
				SystemPropertyUtil.getInt(
						"io.netty.allocator.numDirectArenas",
						(int) Math.min(
								defaultMinNumArena, 
								PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));
		
		DEFAULT_SMALL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.smallCacheSize", 256);
		DEFAULT_NORMAL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.normalCacheSize", 64);
		
		DEFAULT_MAX_CACHED_BUFFER_CAPACITY = SystemPropertyUtil.getInt(
				"io.netty.allocator.maxCachedBufferCapacity", 32 * 1024);
		
		DEFAULT_CACHE_TRIM_INTERVAL = SystemPropertyUtil.getInt(
				"io.netty.allocator.cacheTrimInterval", 8192);
		
		if (SystemPropertyUtil.contains("io.netty.allocation.cacheTrimIntervalMillis")) {
			logger.warn("-Dio.netty.allocation.cacheTrimIntervalMillis is deprecated," + 
						" use -Dio,netty.allocator.cacheTrimIntervalMillis");
			if (SystemPropertyUtil.contains("io.netty.allocator.cacheTrimIntervalMillis")) {
				DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
						"io.netty.allocator.cacheTrimIntervalMillis", 0);
			} else {
				DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
						"io.netty.allocation.cacheTrimIntervalMillis", 0);
			}
		} else {
			DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
					"io.netty.allocation.cacheTrimIntervalMillis", 0);
		}
		
		DEFAULT_USE_CACHE_FOR_ALL_THREADS = SystemPropertyUtil.getBoolean(
				"io.netty.allocator.useCacheForAllThreads", false);
		
		DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS = SystemPropertyUtil.getBoolean(
				"io.netty.allocator.disableCacheFinalizersForFastThreadLocalThreads", false);
		
		DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK = SystemPropertyUtil.getInt(
				"io.netty.allocator.maxCachedByteBuffersPerChunk", 1023);
		
		if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.allocator.numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
            logger.debug("-Dio.netty.allocator.numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);
            if (pageSizeFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE);
            } else {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
            }
            if (maxOrderFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER);
            } else {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
            }
            logger.debug("-Dio.netty.allocator.chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
            logger.debug("-Dio.netty.allocator.smallCacheSize: {}", DEFAULT_SMALL_CACHE_SIZE);
            logger.debug("-Dio.netty.allocator.normalCacheSize: {}", DEFAULT_NORMAL_CACHE_SIZE);
            logger.debug("-Dio.netty.allocator.maxCachedBufferCapacity: {}", DEFAULT_MAX_CACHED_BUFFER_CAPACITY);
            logger.debug("-Dio.netty.allocator.cacheTrimInterval: {}", DEFAULT_CACHE_TRIM_INTERVAL);
            logger.debug("-Dio.netty.allocator.cacheTrimIntervalMillis: {}", DEFAULT_CACHE_TRIM_INTERVAL_MILLIS);
            logger.debug("-Dio.netty.allocator.useCacheForAllThreads: {}", DEFAULT_USE_CACHE_FOR_ALL_THREADS);
            logger.debug("-Dio.netty.allocator.maxCachedByteBuffersPerChunk: {}",
                    DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK);
            logger.debug("-Dio.netty.allocator.disableCacheFinalizersForFastThreadLocalThreads: {}",
                         DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS);
        }
	}
	
	public static final PooledByteBufAllocator DEFAULT = 
			new PooledByteBufAllocator(!PlatformDependent.isExplicitNoPreferDirect());
	
	private final PoolArena<byte[]>[] heapArenas;
	private final PoolArena<ByteBuffer>[] directArenas;
	private final int smallCacheSize;
	private final int normalCacheSize;
	private final List<PoolArenaMetric> heapArenaMetrics;
	private final List<PoolArenaMetric> directArenaMetrics;
	private final PoolThreadLocalCache threadCache;
	private final int chunkSize;
	private final PooledByteBufAllocatorMetric metric;
	
	public PooledByteBufAllocator() {
		this(false);
	}
	
	public PooledByteBufAllocator(boolean preferDirect) {
		this(preferDirect, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
	}
	
	public PooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
		this(false, nHeapArena, nDirectArena, pageSize, maxOrder);
	}
	
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
             0, DEFAULT_SMALL_CACHE_SIZE, DEFAULT_NORMAL_CACHE_SIZE);
    }
    
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
            int tinyCacheSize, int smallCacheSize, int normalCacheSize) {
    	this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder, smallCacheSize,
    			normalCacheSize, DEFAULT_USE_CACHE_FOR_ALL_THREADS, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
    }
    
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena,
            int nDirectArena, int pageSize, int maxOrder, int tinyCacheSize,
            int smallCacheSize, int normalCacheSize,
	            boolean useCacheForAllThreads) {
		this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
			smallCacheSize, normalCacheSize,
			useCacheForAllThreads);
    }
    
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena,
    							  int nDirectArena, int pageSize, int maxOrder,
    							  int smallCacheSize, int normalCacheSize,
    							  boolean useCacheForAllThreads) {
    	this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
    			smallCacheSize, normalCacheSize,
    			useCacheForAllThreads, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
    }
    
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
            int tinyCacheSize, int smallCacheSize, int normalCacheSize,
            boolean useCacheForAllThreads, int directMemoryCacheAlignment) {
		this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
				smallCacheSize, normalCacheSize,
				useCacheForAllThreads, directMemoryCacheAlignment);
	}
    
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
    							  int smallCacheSize, int normalCacheSize,
    							  boolean useCacheForAllThreads, int directMemoryCacheAlignment) {
    	super(preferDirect);
    	threadCache = new PoolThreadLocalCache(useCacheForAllThreads);
    	this.smallCacheSize = smallCacheSize;
    	this.normalCacheSize = normalCacheSize;
    
    	if (directMemoryCacheAlignment != 0) {
    		if (!PlatformDependent.hasAlignDirectByteBuffer()) {
    			throw new UnsupportedOperationException("Buffer alignment is not supported. " +
    					"Either Unsafe or ByteBuffer.alignSlice() must be available.");
    		}
    		
    		pageSize = (int) PlatformDependent.align(pageSize, directMemoryCacheAlignment);
    	}
    	
    	chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);
    	
    	checkPositiveOrZero(nHeapArena, "nHeapArena");
    	checkPositiveOrZero(nDirectArena, "nDirectArena");
    	
    	checkPositiveOrZero(directMemoryCacheAlignment, "directMemoryCacheAlignment");
    	if (directMemoryCacheAlignment > 0 && !isDirectMemoryCacheAlignmentSupported()) {
    		throw new IllegalArgumentException("directMemoryCacheAlignment is not supported");
    	}
    	
    	if ((directMemoryCacheAlignment & -directMemoryCacheAlignment) != directMemoryCacheAlignment) {
    		throw new IllegalArgumentException("directMemoryCacheAlignment: " 
    				+ directMemoryCacheAlignment + " (expected: power of two)");
    	}
    	
    	int pageShifts = validateAndCalculatePageShifts(pageSize, directMemoryCacheAlignment);
    	
    	if (nHeapArena > 0) {
    		heapArenas = newArenaArray(nHeapArena);
    		List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(heapArenas.length);
    		final SizeClasses sizeClasses = new SizeClasses(pageSize, pageShifts, chunkSize, 0);
    		for (int i = 0; i < heapArenas.length; i++) {
    			PoolArena.HeapArena arena = new PoolArena.HeapArena(this, sizeClasses);
    			heapArenas[i] = arena;
    			metrics.add(arena);
    		}
    		heapArenaMetrics = Collections.unmodifiableList(metrics);
    	} else {
    		heapArenas = null;
    		heapArenaMetrics = Collections.emptyList();
    	}
    	
    	if (nDirectArena > 0) {
    		directArenas = newArenaArray(nDirectArena);
    		List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(directArenas.length);
    		final SizeClasses sizeClasses = new SizeClasses(pageSize, pageShifts, chunkSize,
    				directMemoryCacheAlignment);
    		for (int i = 0; i < directArenas.length; i++) {
    			PoolArena.DirectArena arena = new PoolArena.DirectArena(this, sizeClasses);
    			directArenas[i] = arena;
    			metrics.add(arena);
    		}
    		directArenaMetrics = Collections.unmodifiableList(metrics);
    	} else {
    		directArenas = null;
    		directArenaMetrics = Collections.emptyList();
    	}
    	metric = new PooledByteBufAllocatorMetric(this);
    }
    
    @SuppressWarnings("unchecked")
	private static <T> PoolArena<T>[] newArenaArray(int size) {
    	return new PoolArena[size];
    }
    
    private static int validateAndCalculatePageShifts(int pageSize, int alignment) {
    	if (pageSize < MIN_PAGE_SIZE) {
    		throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ')');
    	}
    	
    	if ((pageSize & pageSize - 1) != 0) {
    		throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
    	}
    	
    	if (pageSize < alignment) {
    		throw new IllegalArgumentException("Alignment cannot be greater than page size. " +
    				"Alignment: " + alignment + ", page size: " + pageSize + '.');
    	}
    	
    	return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }
    
    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
    	if (maxOrder > 14) {
    		throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
    	}
    	
    	int chunkSize = pageSize;
    	for (int i = maxOrder; i > 0; i--) {
    		if (chunkSize > MAX_CHUNK_SIZE / 2) {
    			throw new IllegalArgumentException(String.format(
    					"pageSize (%d) << maxOrder (%d) must not be exceed %d",
    					pageSize, maxOrder, MAX_CHUNK_SIZE));
    		}
    		chunkSize <<= 1;
    	}
    	return chunkSize;
    }
    
    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    	PoolThreadCache cache = threadCache.get();
    	PoolArena<byte[]> heapArena = cache.heapArena;
    	
    	final AbstractByteBuf buf;
    	if (heapArena != null) {
    		buf = heapArena.allocate(cache, initialCapacity, maxCapacity);
    	} else {
    		buf = PlatformDependent.hasUnsafe() ?
    				new UnpooledUnsafeHeapByteBuf(this, initialCapacity, maxCapacity) :
    				new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
    		onAllocateBuffer(buf, false, false);
    	}
    	return toLeakAwareBuffer(buf);
    } 
    
    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    	PoolThreadCache cache = threadCache.get();
    	PoolArena<ByteBuffer> directArena = cache.directArena;
    	
    	final AbstractByteBuf buf;
    	if (directArena != null) {
    		buf = directArena.allocate(cache, initialCapacity, maxCapacity);
    	} else {
    		buf = UnsafeByteBufUtil.newDirectByteBuf(this, initialCapacity, maxCapacity);
    		onAllocateBuffer(buf, false, false);
    	}
    	return toLeakAwareBuffer(buf);
    }
    
    public static int defaultNumHeapArena() {
    	return DEFAULT_NUM_HEAP_ARENA;
    }
    
    public static int defaultNumDirectArena() {
    	return DEFAULT_NUM_DIRECT_ARENA;
    }
    
    public static int defaultPageSize() {
    	return DEFAULT_PAGE_SIZE;
    }
    
    public static int defaultMaxOrder() {
    	return DEFAULT_MAX_ORDER;
    }
    
    public static boolean defaultDisableCacheFinalizersForFastThreadLocalThreads() {
    	return DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS;
    }
    
    public static boolean defaultUseCacheForAllThreads() {
    	return DEFAULT_USE_CACHE_FOR_ALL_THREADS;
    }
    
    public static boolean defaultPreferDirect() {
    	return PlatformDependent.directBufferPreferred();
    }
    
    public static int defaultTinyCacheSize() {
    	return 0;
    }
    
    public static int defaultSmallCacheSize() {
    	return DEFAULT_SMALL_CACHE_SIZE;
    }
    
    public static int defaultNormalCacheSize() {
    	return DEFAULT_NORMAL_CACHE_SIZE;
    }
    
    public static boolean isDirectMemoryCacheAlignmentSupported() {
    	return PlatformDependent.hasUnsafe();
    }
    
    @Override
    public boolean isDirectBufferPooled() {
    	return directArenas != null;
    }
    
    public boolean hasThreadLocalCache() {
    	return threadCache.isSet();
    }
    
    public void freeThreadLocalCache() {
    	threadCache.remove();
    }
    
    private final class PoolThreadLocalCache extends FastThreadLocal<PoolThreadCache> {
    	private final boolean useCacheForAllThreads;
    	
    	PoolThreadLocalCache(boolean useCacheForAllThreads) {
    		this.useCacheForAllThreads = useCacheForAllThreads;
    	}
    	
    	@Override
    	protected synchronized PoolThreadCache initialValue() {
    		final PoolArena<byte[]> heapArena = leastUsedArena(heapArenas);
    		final PoolArena<ByteBuffer> directArena = leastUsedArena(directArenas);
    		
    		@SuppressWarnings("unused")
			final Thread current = Thread.currentThread();
    		final EventExecutor executor = ThreadExecutorMap.currentExecutor();
    		
    		
    		if (useCacheForAllThreads || 
    				FastThreadLocalThread.currentThreadHasFastThreadLocal() ||
    				executor != null) {
    			final PoolThreadCache cache = new PoolThreadCache(
    					heapArena, directArena, smallCacheSize, normalCacheSize,
    					DEFAULT_MAX_CACHED_BUFFER_CAPACITY, DEFAULT_CACHE_TRIM_INTERVAL, useCacheFinalizers());
    			
    			if (DEFAULT_CACHE_TRIM_INTERVAL_MILLIS > 0) {
    				if (executor != null) {
    					executor.scheduleAtFixedRate(trimTask, DEFAULT_CACHE_TRIM_INTERVAL_MILLIS, 
    							DEFAULT_CACHE_TRIM_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    				}
    			}
    			return cache;
    		}
    		return new PoolThreadCache(heapArena, directArena, 0, 0, 0, 0, false);
    	}
    	
    	@Override
    	protected void onRemoval(PoolThreadCache threadCache) {
    		threadCache.free(false);
    	}
    	
    	private <T> PoolArena<T> leastUsedArena(PoolArena<T>[] arenas) {
    		if (arenas == null || arenas.length == 0) {
    			return null;
    		}
    		
    		PoolArena<T> minArena = arenas[0];
    		
    		if (minArena.numThreadCaches.get() == CACHE_NOT_USED) {
    			return minArena;
    		}
    		for (int i = 0; i < arenas.length; i++) {
    			PoolArena<T> arena = arenas[i];
    			if (arena.numThreadCaches.get() < minArena.numThreadCaches.get()) {
    				minArena = arena;
    			}
    		}
    		
    		return minArena;
    	}
    }
    
    private static boolean useCacheFinalizers() {
    	if (!defaultDisableCacheFinalizersForFastThreadLocalThreads()) {
    		return true;
    	}
    	return FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals();
    }
    
    @Override
    public PooledByteBufAllocatorMetric metric() {
    	return metric;
    }
    
    public int numHeapArenas() {
    	return heapArenaMetrics.size();
    }
    
    public int numDirectArenas() {
        return directArenaMetrics.size();
    }
    
    public List<PoolArenaMetric> heapArenas() {
        return heapArenaMetrics;
    }
    
    public List<PoolArenaMetric> directArenas() {
        return directArenaMetrics;
    }

    public int numThreadLocalCaches() {
        return Math.max(numThreadLocalCaches(heapArenas), numThreadLocalCaches(directArenas));
    }

    
    private static int numThreadLocalCaches(PoolArena<?>[] arenas) {
    	if (arenas == null) {
    		return 0;
    	}
    	int total = 0;
    	for (PoolArena<?> arena : arenas) {
    		total += arena.numThreadCaches.get();
    	}
    	return total;
    }
    
    public int tinyCacheSize() {
    	return 0;
    }
    
    public int smallCacheSize() {
    	return smallCacheSize;
    }
    
    public int normalCacheSize() {
    	return normalCacheSize;
    }
    
    public final int chunkSize() {
    	return chunkSize;
    }
    
    final long usedHeapMemory() {
    	return usedMemory(heapArenas);
    }
    
    final long usedDirectMemory() {
    	return usedMemory(directArenas);
    }
    
    private static long usedMemory(PoolArena<?>[] arenas) {
    	if (arenas == null) {
    		return -1;
    	}
    	long used = 0;
    	for (PoolArena<?> arena : arenas) {
    		used += arena.numActiveBytes();
    		if (used < 0) {
    			return Long.MAX_VALUE;
    		}
    	}
    	return used;
    }
    
    public final long pinnedHeapMemory() {
    	return pinnedMemory(heapArenas);
    }
    
    public final long pinnedDirectMemory() {
    	return pinnedMemory(directArenas);
    }
    
    private static long pinnedMemory(PoolArena<?>[] arenas) {
    	if (arenas == null) {
    		return -1;
    	}
    	long used = 0;
    	for (PoolArena<?> arena : arenas) {
    		used += arena.numPinnedBytes();
    		if (used < 0) {
    			return Long.MAX_VALUE;
    		}
    	}
    	return used;
    }
    
    final PoolThreadCache threadCache() {
    	PoolThreadCache cache = threadCache.get();
    	assert cache != null;
    	return cache;
    }
    
    public boolean trimCurrentThreadCache() {
    	PoolThreadCache cache = threadCache.getIfExists();
    	if (cache != null) {
    		cache.trim();
    		return true;
    	}
    	return false;
    }
    
    public String dumpStats() {
        int heapArenasLen = heapArenas == null ? 0 : heapArenas.length;
        StringBuilder buf = new StringBuilder(512)
                .append(heapArenasLen)
                .append(" heap arena(s):")
                .append(StringUtil.NEWLINE);
        if (heapArenasLen > 0) {
            for (PoolArena<byte[]> a: heapArenas) {
                buf.append(a);
            }
        }

        int directArenasLen = directArenas == null ? 0 : directArenas.length;

        buf.append(directArenasLen)
           .append(" direct arena(s):")
           .append(StringUtil.NEWLINE);
        if (directArenasLen > 0) {
            for (PoolArena<ByteBuffer> a: directArenas) {
                buf.append(a);
            }
        }

        return buf.toString();
    }
    
    static void onAllocateBuffer(AbstractByteBuf buf, boolean pooled, boolean threadLocal) {
    	if (PlatformDependent.isJfrEnabled() && AllocateBufferEvent.isEventEnabled()) {
    		AllocateBufferEvent event = new AllocateBufferEvent();
    		if (event.shouldCommit()) {
    			event.fill(buf, PooledByteBufAllocator.class);
    			event.chunkPooled = pooled;
    			event.chunkThreadLocal = threadLocal;
    			event.commit();
    		}
    	}
    }
    
    static void onDeallocateBuffer(AbstractByteBuf buf) {
    	if (PlatformDependent.isJfrEnabled() && FreeBufferEvent.isEventEnabled()) {
    		FreeBufferEvent event = new FreeBufferEvent();
    		if (event.shouldCommit()) {
    			event.fill(buf, PooledByteBufAllocator.class);
    			event.commit();
    		}
    	}
    }
    
    static void onReallocateBuffer(AbstractByteBuf buf, int newCapacity) {
        if (PlatformDependent.isJfrEnabled() && ReallocateBufferEvent.isEventEnabled()) {
            ReallocateBufferEvent event = new ReallocateBufferEvent();
            if (event.shouldCommit()) {
                event.fill(buf, PooledByteBufAllocator.class);
                event.newCapacity = newCapacity;
                event.commit();
            }
        }
    }

    static void onAllocateChunk(ChunkInfo chunk, boolean pooled) {
        if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
            AllocateChunkEvent event = new AllocateChunkEvent();
            if (event.shouldCommit()) {
                event.fill(chunk, PooledByteBufAllocator.class);
                event.pooled = pooled;
                event.threadLocal = false;
                event.commit();
            }
        }
    }

    static void onDeallocateChunk(ChunkInfo chunk, boolean pooled) {
        if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
            FreeChunkEvent event = new FreeChunkEvent();
            if (event.shouldCommit()) {
                event.fill(chunk, PooledByteBufAllocator.class);
                event.pooled = pooled;
                event.commit();
            }
        }
    }
}  
