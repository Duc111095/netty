package netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import netty.buffer.PoolArena.SizeClass;
import netty.common.util.Recycler;
import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.internal.MathUtil;
import netty.common.util.internal.ObjectPool.Handle;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkPositiveOrZero;

final class PoolThreadCache {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolThreadCache.class);
	private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;
	
	final PoolArena<byte[]> heapArena;
	final PoolArena<ByteBuffer> directArena;
	
	private final MemoryRegionCache<byte[]>[] smallSubPageHeapCaches;
	private final MemoryRegionCache<ByteBuffer>[] smallSubPageDirectCaches;
	private final MemoryRegionCache<byte[]>[] normalHeapCaches;
	private final MemoryRegionCache<ByteBuffer>[] normalDirectCaches;
	
	private final int freeSweepAllocationThreshold;
	private final AtomicBoolean freed = new AtomicBoolean();
	
	@SuppressWarnings("unused")
	private final FreeOnFinalize freeOnFinalize;
	
	private int allocations;
	
	PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
            int smallCacheSize, int normalCacheSize, int maxCachedBufferCapacity,
            int freeSweepAllocationThreshold, boolean useFinalizer) {
		checkPositiveOrZero(maxCachedBufferCapacity, "maxCachedBufferCapacity");
		this.freeSweepAllocationThreshold = freeSweepAllocationThreshold;
		this.heapArena = heapArena;
		this.directArena = directArena;
		if (directArena != null) {
		    smallSubPageDirectCaches = createSubPageCaches(smallCacheSize, directArena.sizeClass.nSubpages);
		    normalDirectCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, directArena);
		    directArena.numThreadCaches.getAndIncrement();
		} else {
		    // No directArea is configured so just null out all caches
		    smallSubPageDirectCaches = null;
		    normalDirectCaches = null;
		}
		if (heapArena != null) {
		    // Create the caches for the heap allocations
		    smallSubPageHeapCaches = createSubPageCaches(smallCacheSize, heapArena.sizeClass.nSubpages);
		    normalHeapCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, heapArena);
		    heapArena.numThreadCaches.getAndIncrement();
		} else {
		    // No heapArea is configured so just null out all caches
		    smallSubPageHeapCaches = null;
		    normalHeapCaches = null;
		}
		
		// Only check if there are caches in use.
		if ((smallSubPageDirectCaches != null || normalDirectCaches != null
		        || smallSubPageHeapCaches != null || normalHeapCaches != null)
		        && freeSweepAllocationThreshold < 1) {
		    throw new IllegalArgumentException("freeSweepAllocationThreshold: "
		            + freeSweepAllocationThreshold + " (expected: > 0)");
		}
		freeOnFinalize = useFinalizer ? new FreeOnFinalize(this) : null;
	}
	
	private static <T> MemoryRegionCache<T>[] createSubPageCaches(
			int cacheSize, int numCaches) {
		if (cacheSize > 0 && numCaches > 0) {
			@SuppressWarnings("unchecked")
			MemoryRegionCache<T>[] cache = new MemoryRegionCache[numCaches];
			for (int i = 0; i < cache.length; i++) {
				cache[i] = new SubPageMemoryRegionCache<T>(cacheSize);
			}
			return cache;
		} else {
			return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	private static <T> MemoryRegionCache<T>[] createNormalCaches(
			int cacheSize, int maxCachedBufferCapacity, PoolArena<T> area) {
		if (cacheSize > 0 && maxCachedBufferCapacity > 0) {
			int max = Math.min(area.sizeClass.chunkSize, maxCachedBufferCapacity);
			List<MemoryRegionCache<T>> cache = new ArrayList<MemoryRegionCache<T>>();
			for (int idx = area.sizeClass.nSubpages; idx < area.sizeClass.nSizes &&
					area.sizeClass.sizeIdx2size(idx) <= max; idx++) {
				cache.add(new NormalMemoryRegionCache<T>(cacheSize));
			}
			return cache.toArray(new MemoryRegionCache[0]);
		} else {
			return null;
		}
	}
	
	static int log2(int val) {
		return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
	}
	
	boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int sizeIdx) {
		return allocate(cacheForSmall(area, sizeIdx), buf, reqCapacity);
	}
	
	boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int sizeIdx) {
		return allocate(cacheForNormal(area, sizeIdx), buf, reqCapacity);
	}
	
	private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
		if (cache == null) {
			return false;
		}
		boolean allocated = cache.allocate(buf, reqCapacity, this);
		if (++allocations >= freeSweepAllocationThreshold) {
			allocations = 0;
			trim();
		}
		return allocated;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	boolean add(PoolArena<?> area, PoolChunk chunk, ByteBuffer nioBuffer,
			long handle, int normCapacity, SizeClass sizeClass) {
		int sizeIdx = area.sizeClass.size2SizeIdx(normCapacity);
		MemoryRegionCache<?> cache = cache(area, sizeIdx, sizeClass);
		if (cache == null) {
			return false;
		}
		if (freed.get()) {
			return false;
		}
		return cache.add(chunk, nioBuffer, handle, normCapacity);
	}
	
	private MemoryRegionCache<?> cache(PoolArena<?> area, int sizeIdx, SizeClass sizeClass) {
		switch (sizeClass) {
		case Normal:
			return cacheForNormal(area, sizeIdx);
		case Small:
			return cacheForSmall(area, sizeIdx);
		default:
			throw new Error("Unexpected size class: " + sizeClass);
		}
	}
    
    void free(boolean finalizer) {
        // As free() may be called either by the finalizer or by FastThreadLocal.onRemoval(...) we need to ensure
        // we only call this one time.
        if (freed.compareAndSet(false, true)) {
            if (freeOnFinalize != null) {
                // Help GC: this can race with a finalizer thread, but will be null out regardless
                freeOnFinalize.cache = null;
            }
            int numFreed = free(smallSubPageDirectCaches, finalizer) +
                           free(normalDirectCaches, finalizer) +
                           free(smallSubPageHeapCaches, finalizer) +
                           free(normalHeapCaches, finalizer);

            if (numFreed > 0 && logger.isDebugEnabled()) {
                logger.debug("Freed {} thread-local buffer(s) from thread: {}", numFreed,
                             Thread.currentThread().getName());
            }

            if (directArena != null) {
                directArena.numThreadCaches.getAndDecrement();
            }

            if (heapArena != null) {
                heapArena.numThreadCaches.getAndDecrement();
            }
        }
    }

    private static int free(MemoryRegionCache<?>[] caches, boolean finalizer) {
        if (caches == null) {
            return 0;
        }

        int numFreed = 0;
        for (MemoryRegionCache<?> c: caches) {
            numFreed += free(c, finalizer);
        }
        return numFreed;
    }

    private static int free(MemoryRegionCache<?> cache, boolean finalizer) {
        if (cache == null) {
            return 0;
        }
        return cache.free(finalizer);
    }
    
    void trim() {
        trim(smallSubPageDirectCaches);
        trim(normalDirectCaches);
        trim(smallSubPageHeapCaches);
        trim(normalHeapCaches);
    }

    private static void trim(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (MemoryRegionCache<?> c: caches) {
            trim(c);
        }
    }

    private static void trim(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.trim();
    }

    private MemoryRegionCache<?> cacheForSmall(PoolArena<?> area, int sizeIdx) {
        if (area.isDirect()) {
            return cache(smallSubPageDirectCaches, sizeIdx);
        }
        return cache(smallSubPageHeapCaches, sizeIdx);
    }

    private MemoryRegionCache<?> cacheForNormal(PoolArena<?> area, int sizeIdx) {
        // We need to subtract area.sizeClass.nSubpages as sizeIdx is the overall index for all sizes.
        int idx = sizeIdx - area.sizeClass.nSubpages;
        if (area.isDirect()) {
            return cache(normalDirectCaches, idx);
        }
        return cache(normalHeapCaches, idx);
    }

    private static <T> MemoryRegionCache<T> cache(MemoryRegionCache<T>[] cache, int sizeIdx) {
        if (cache == null || sizeIdx >= cache.length) {
            return null;
        }
        return cache[sizeIdx];
    }
	
	private static final class SubPageMemoryRegionCache<T> extends MemoryRegionCache<T> {
		SubPageMemoryRegionCache(int size) {
			super(size, SizeClass.Small);
		}
		
		@Override
		protected void initBuf(
				PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, PooledByteBuf<T> buf, int reqCapacity,
				PoolThreadCache threadCache) {
			chunk.initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache, true);
		}
	}
	
	private static final class NormalMemoryRegionCache<T> extends MemoryRegionCache<T> {
        NormalMemoryRegionCache(int size) {
            super(size, SizeClass.Normal);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, PooledByteBuf<T> buf, int reqCapacity,
                PoolThreadCache threadCache) {
            chunk.initBuf(buf, nioBuffer, handle, reqCapacity, threadCache, true);
        }
    }
	
	private abstract static class MemoryRegionCache<T> {
		private final int size;
		private final Queue<Entry<T>> queue;
		private final SizeClass sizeClass;
		private int allocations;
		
		MemoryRegionCache(int size, SizeClass sizeClass) {
			this.size = MathUtil.safeFindNextPositivePowerOfTwo(size);
			queue = PlatformDependent.newFixedMpscUnpaddedQueue(this.size);
			this.sizeClass = sizeClass;
		}
		
		protected abstract void initBuf(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle,
				PooledByteBuf<T> buf, int reqCapacity, PoolThreadCache threadCache);
		
		public final boolean add(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, long normCapacity) {
			Entry<T> entry = newEntry(chunk, nioBuffer, handle, normCapacity);
			boolean queued = queue.offer(entry);
			if (!queued) {
				entry.unguardedRecycle();
			}
			
			return queued;
		}
		
		public final boolean allocate(PooledByteBuf<T> buf, int reqCapacity, PoolThreadCache threadCache) {
			Entry<T> entry = queue.poll();
			if (entry == null) {
				return false;
			}
			initBuf(entry.chunk, entry.nioBuffer, entry.handle, buf, reqCapacity, threadCache);
			entry.unguardedRecycle();
			
			++allocations;
			return true;
		}
		
		public final int free(boolean finalizer) {
			return free(Integer.MAX_VALUE, finalizer);
		}
		
		private int free(int max, boolean finalizer) {
			int numFreed = 0;
			for (; numFreed < max; numFreed++) {
				Entry<T> entry = queue.poll();
				if (entry != null) {
					freeEntry(entry, finalizer);
				} else {
					return numFreed;
				}
			}
			return numFreed;
		}
		
		public final void trim() {
			int free = size - allocations;
			allocations = 0;
			if (free > 0) {
				free(free, false);
			}
		}
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void freeEntry(Entry entry, boolean finalizer) {
			PoolChunk chunk = entry.chunk;
			long handle = entry.handle;
			ByteBuffer nioBuffer = entry.nioBuffer;
			int normCapacity = entry.normCapacity;
			
			if (!finalizer) {
				entry.recycle();
			}
			
			chunk.arena.freeChunk(chunk, handle, normCapacity, sizeClass, nioBuffer, finalizer);
		}
		
		static final class Entry<T> {
			final EnhancedHandle<Entry<?>> recyclerHandle;
			PoolChunk<T> chunk;
			ByteBuffer nioBuffer;
			long handle = -1;
			int normCapacity;
			
			Entry(Handle<Entry<?>> recyclerHandle) {
				this.recyclerHandle = (EnhancedHandle<Entry<?>>) recyclerHandle;
			}
			
			void recycle() {
				chunk = null;
				nioBuffer = null;
				handle = -1;
				recyclerHandle.recycle(this);
			}
			
			void unguardedRecycle() {
				chunk = null;
				nioBuffer = null;
				handle = -1;
				recyclerHandle.unguardedRecycle(this);
			}
		}
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		private static Entry newEntry(PoolChunk<?> chunk, ByteBuffer nioBuffer, long handle, int normCapacity) {
			Entry entry = RECYCLER.get();
			entry.chunk = chunk;
			entry.nioBuffer = nioBuffer;
			entry.handle = handle;
			entry.normCapacity = normCapacity;
			return entry;
		}
		
		@SuppressWarnings("rawtypes")
		private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
			@Override
			@SuppressWarnings("unchecked")
			protected Entry newObject(Handle<Entry> handle) {
				return new Entry(handle);
			}
		};
	}
	
	private static final class FreeOnFinalize {
		
		private volatile PoolThreadCache cache;
		
		private FreeOnFinalize(PoolThreadCache cache) {
			this.cache = cache;
		}
		
		@SuppressWarnings("deprecation")
		protected void finalize() throws Throwable {
			try {
				PoolThreadCache cache = this.cache;
				this.cache = null;
				if (cache != null) {
					cache.free(true);
				}
			} finally {
				super.finalize();
			}
		}
	}
}
