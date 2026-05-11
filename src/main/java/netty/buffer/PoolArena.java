package netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import netty.common.util.internal.CleanableDirectBuffer;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;

import static netty.buffer.PoolChunk.isSubpage;
import static java.lang.Math.max;

abstract class PoolArena<T> implements PoolArenaMetric {
	private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();
	
	enum SizeClass {
		Small,
		Normal
	}
	
	final PooledByteBufAllocator parent;
	
	final PoolSubpage<T>[] smallSubpagePools;
	
	private final PoolChunkList<T> q050;
	private final PoolChunkList<T> q025;
	private final PoolChunkList<T> q000;
	private final PoolChunkList<T> qInit;
	private final PoolChunkList<T> q075;
	private final PoolChunkList<T> q100;
	
	private final List<PoolChunkListMetric> chunkListMetrics;
	
	private long allocationsNormal;
	private final LongAdder allocationsSmall = new LongAdder();
	private final LongAdder allocationsHuge = new LongAdder();
	private final LongAdder activeBytesHuge = new LongAdder();
	
	private long deallocationsSmall;
	private long deallocationsNormal;
	
	private long pooledChunkAllocations;
	private long pooledChunkDeallocations;
	
	private final LongAdder deallocationsHuge = new LongAdder();
	
	final AtomicInteger numThreadCaches = new AtomicInteger();
	
	private final ReentrantLock lock = new ReentrantLock();

	final SizeClasses sizeClass;
	
	protected PoolArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
		assert null != sizeClass;
		this.parent = parent;
		this.sizeClass = sizeClass;
		smallSubpagePools = newSubpagePoolArray(sizeClass.nSubpages);
		for (int i = 0; i < smallSubpagePools.length; i++) {
			smallSubpagePools[i] = newSubpagePoolHead(i);
		}
		
		q100 = new PoolChunkList<T>(this, null, 100, Integer.MAX_VALUE, sizeClass.chunkSize);
		q075 = new PoolChunkList<T>(this, q100, 75, 100, sizeClass.chunkSize);
		q050 = new PoolChunkList<T>(this, q100, 50, 100, sizeClass.chunkSize);
		q025 = new PoolChunkList<T>(this, q050, 25, 75, sizeClass.chunkSize);
		q000 = new PoolChunkList<T>(this, q025, 1, 50, sizeClass.chunkSize);
		qInit = new PoolChunkList<T>(this, q000, Integer.MIN_VALUE, 25, sizeClass.chunkSize);
		
		q100.prevList(q075);
		q075.prevList(q050);
		q050.prevList(q025);
		q025.prevList(q000);
		q000.prevList(null);
		qInit.prevList(qInit);
		
		List<PoolChunkListMetric> metrics = new ArrayList<PoolChunkListMetric>(6);
		metrics.add(qInit);
		metrics.add(q000);
		metrics.add(q025);
		metrics.add(q050);
		metrics.add(q075);
		metrics.add(q100);
		chunkListMetrics = Collections.unmodifiableList(metrics);
	}
	
	private PoolSubpage<T> newSubpagePoolHead(int index) {
		PoolSubpage<T> head = new PoolSubpage<T>(index);
		head.prev = head;
		head.next = head;
		return head;
	}
	
	@SuppressWarnings("unchecked")
	private PoolSubpage<T>[] newSubpagePoolArray(int size) {
		return new PoolSubpage[size];
	}
	
	abstract boolean isDirect();
	
	PooledByteBuf<T> allocate(PoolThreadCache cache, int reqCapacity, int maxCapacity) {
		PooledByteBuf<T> buf = newByteBuf(maxCapacity);
		allocate(cache, buf, reqCapacity);
		return buf;
	}
	
	private void allocate(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity) {
		final int sizeIdx = sizeClass.size2SizeIdx(reqCapacity);
		
		if (sizeIdx <= sizeClass.smallMaxSizeIdx) {
			tcacheAllocateSmall(cache, buf, reqCapacity, sizeIdx);
		} else if (sizeIdx < sizeClass.nSizes) {
			tcacheAllocateNormal(cache, buf, reqCapacity, sizeIdx);
		} else {
			int normCapacity = sizeClass.directMemoryCacheAlignment > 0
				? sizeClass.normalizeSize(reqCapacity) : reqCapacity;
			allocateHuge(buf, normCapacity);
		}
	}
	
	private void tcacheAllocateSmall(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity,
			final int sizeIdx) {
		if (cache.allocateSmall(this, buf, reqCapacity, sizeIdx)) {
			return;
		}
		
		final PoolSubpage<T> head = smallSubpagePools[sizeIdx];
		final boolean needsNormalAllocation;
		head.lock();
		try {
			final PoolSubpage<T> s = head.next;
			needsNormalAllocation = s == head;
			if (!needsNormalAllocation) {
				assert s.doNotDestroy && s.elemSize == sizeClass.sizeIdx2size(sizeIdx) : "doNotDestroy=" + 
						s.doNotDestroy + ", elemSize=" + s.elemSize + ", sizeIdx=" + sizeIdx;
				long handle = s.allocate();
				assert handle >= 0;
				s.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache, false);
			}
		} finally {
			head.unlock();
		}
		
		if (needsNormalAllocation) {
			lock();
			try {
				allocateNormal(buf, reqCapacity, sizeIdx, cache);
			} finally {
				unlock();
			}
		}
		
		incSmallAllocation();
	}
	
	private void tcacheAllocateNormal(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity,
			final int sizeIdx) {
		if (cache.allocateNormal(this, buf, reqCapacity, sizeIdx)) {
			return;
		}
		lock();
		try {
			allocateNormal(buf, reqCapacity, sizeIdx, cache);
			++allocationsNormal;
		} finally {
			unlock();
		}
	}
	
	private void allocateNormal(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
		assert lock.isHeldByCurrentThread();
		if (q050.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
			q025.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
			q000.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
			qInit.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
			q075.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
			return;
		}
		
		PoolChunk<T> c = newChunk(sizeClass.pageSize, sizeClass.nPSizes, sizeClass.pageShifts, sizeClass.chunkSize);
		PooledByteBufAllocator.onAllocateChunk(c, true);
		boolean success = c.allocate(buf, reqCapacity, sizeIdx, threadCache);
		assert success;
		qInit.add(c);
		++pooledChunkAllocations;
	}
	
	private void incSmallAllocation() {
		allocationsSmall.increment();
	}
	
	private void allocateHuge(PooledByteBuf<T> buf, int reqCapacity) {
		PoolChunk<T> chunk = newUnpooledChunk(reqCapacity);
		PooledByteBufAllocator.onAllocateChunk(chunk, false);
		activeBytesHuge.add(chunk.chunkSize());
		buf.initUnpooled(chunk, reqCapacity);
		allocationsHuge.increment();
	}
	
	void free(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int normCapacity, PoolThreadCache cache) {
		chunk.decrementPinnedMemory(normCapacity);
		if (chunk.unpooled) {
			int size = chunk.chunkSize();
			destroyChunk(chunk);
			activeBytesHuge.add(-size);
			deallocationsHuge.increment();
		} else {
			SizeClass sizeClass = sizeClass(handle);
			if (cache != null && cache.add(this, chunk, nioBuffer, handle, normCapacity, sizeClass)) {
				return;
			}
			
			freeChunk(chunk, handle, normCapacity, sizeClass, nioBuffer, false);
		}
	}
	
	private static SizeClass sizeClass(long handle) {
		return isSubpage(handle) ? SizeClass.Small : SizeClass.Normal;
	}
	
	void freeChunk(PoolChunk<T> chunk, long handle, int normCapacity, SizeClass sizeClass, ByteBuffer nioBuffer,
			boolean finalizer) {
		final boolean destroyChunk;
		lock();
		try {
			if (!finalizer) {
				switch (sizeClass) {
				case Normal:
					++deallocationsNormal;
					break;
				case Small:
					++deallocationsSmall;
					break;
				default:
					throw new Error("Unexpected size class: " + sizeClass);
				}
			}
			
			destroyChunk = !chunk.parent.free(chunk, handle, normCapacity, nioBuffer);
			if (destroyChunk) {
				++pooledChunkDeallocations;
			}
		} finally {
			unlock();
		}
		if (destroyChunk) {
			destroyChunk(chunk);
		}
	}
	
	void reallocate(PooledByteBuf<T> buf, int newCapacity) {
		assert newCapacity >= 0 && newCapacity <= buf.maxCapacity();
		
		final int oldCapacity;
		final PoolChunk<T> oldChunk;
		final ByteBuffer oldNioBuffer;
		final long oldHandle;
		final T oldMemory;
		final int oldOffset;
		final int oldMaxLength;
		final PoolThreadCache oldCache;
		
		synchronized (buf) {
			oldCapacity = buf.length;
			if (oldCapacity == newCapacity) {
				return;
			}
			
			oldChunk = buf.chunk;
			oldNioBuffer = buf.tmpNioBuf;
			oldHandle = buf.handle;
			oldMemory = buf.memory;
			oldOffset = buf.offset;
			oldMaxLength = buf.maxLength;
			oldCache = buf.cache;
			
			allocate(parent.threadCache(), buf, newCapacity);
		}
		int bytesToCopy;
		if (newCapacity > oldCapacity) {
			bytesToCopy = oldCapacity;
		} else {
			buf.trimIndiciesToCapacity(newCapacity);
			bytesToCopy = newCapacity;
		}
		memoryCopy(oldMemory, oldOffset, buf, bytesToCopy);
		free(oldChunk, oldNioBuffer, oldHandle, oldMaxLength, oldCache);
	}
	
	@Override
	public int numThreadCaches() {
		return numThreadCaches.get();
	}
	
	@Override
	public int numTinySubpages() {
		return 0;
	}
	
	@Override
	public int numSmallSubpages() {
		return smallSubpagePools.length;
	}
	
	@Override
	public int numChunkLists() {
		return chunkListMetrics.size();
	}
	
	@Override
	public List<PoolSubpageMetric> tinySubpages() {
		return Collections.emptyList();
	}
	
	@Override
	public List<PoolSubpageMetric> smallSubpages() {
		return subPageMetricList(smallSubpagePools);
	}
	
	@Override
	public List<PoolChunkListMetric> chunkLists() {
		return chunkListMetrics;
	}
	
	private static List<PoolSubpageMetric> subPageMetricList(PoolSubpage<?>[] pages) {
		List<PoolSubpageMetric> metrics = new ArrayList<PoolSubpageMetric>();
		for (PoolSubpage<?> head : pages) {
			if (head.next == head) {
				continue;
			}
			PoolSubpage<?> s = head.next;
			for (;;) {
				metrics.add(s);
				s = s.next;
				if (s == head) {
					break;
				}
			}
		}
		return metrics;
	}
	
	@Override
	public long numAllocations() {
		final long allocsNormal;
		lock();
		try {
			allocsNormal = allocationsNormal; 
		} finally {
			unlock();
		}
		
		return allocationsSmall.sum() + allocsNormal + allocationsHuge.sum();
	}
	
	@Override
	public long numTinyAllocations() {
		return 0;
	}
	
	@Override
	public long numSmallAllocations() {
		return allocationsSmall.sum();
	}
	
	@Override
	public long numNormalAllocations() {
		lock();
		try {
			return allocationsNormal;
		} finally {
			unlock();
		}
	}
	
	@Override
	public long numChunkAllocations() {
		lock();
		try {
			return pooledChunkAllocations;
		} finally {
			unlock();
		}
	}
	
	@Override
	public long numDeallocations() {
		final long deallocs;
		lock();
		try {
			deallocs = deallocationsSmall + deallocationsNormal;
		} finally {
			unlock();
		}
		return deallocs + deallocationsHuge.sum();
	}
	
	@Override
	public long numTinyDeallocations() {
		return 0;
	}
	
	@Override
	public long numSmallDeallocations() {
		lock();
		try {
			return deallocationsSmall;
		} finally {
			unlock();
		}
	}
	
	@Override
	public long numNormalDeallocations() {
		lock();
		try {
			return deallocationsNormal;
		} finally {
			unlock();
		}
	}
	
	@Override
	public long numChunkDeallocations() {
		lock();
		try {
			return pooledChunkDeallocations;
		} finally {
			unlock();
		}
	}
	
	@Override
	public long numHugeAllocations() {
		return allocationsHuge.sum();
	}
	
	@Override
	public long numHugeDeallocations() {
		return deallocationsHuge.sum();
	}
	
	@Override
	public long numActiveAllocations() {
		long val = allocationsSmall.sum() + allocationsHuge.sum() 
			- deallocationsHuge.sum();
		lock();
		try {
			val += allocationsNormal - (deallocationsSmall + deallocationsNormal);
		} finally {
			unlock();
		}
		return max(val, 0);
	}
	
	@Override
	public long numActiveTinyAllocations() {
		return 0;
	}
	
	@Override
	public long numActiveSmallAllocations() {
		return max(numSmallAllocations() - numSmallDeallocations(), 0);
	}
	
	@Override
	public long numActiveNormalAllocations() {
		final long val;
		lock();
		try {
			val = allocationsNormal - deallocationsNormal;
		} finally {
			unlock();
		}
		return max(val, 0);
	}
	
	@Override
	public long numActiveChunks() {
		final long val;
		lock();
		try {
			val = pooledChunkAllocations - pooledChunkDeallocations;
		} finally {
			unlock();
		}
		return max(val, 0);
	}
	
	@Override
	public long numActiveHugeAllocations() {
		return max(numHugeAllocations() - numHugeDeallocations(), 0);
	}
	
	@Override
	public long numActiveBytes() {
		long val = activeBytesHuge.sum();
		lock();
		try {
			for (int i = 0; i < chunkListMetrics.size(); i++) {
				for (PoolChunkMetric m : chunkListMetrics.get(i)) {
					val += m.chunkSize();
				}
			}
		} finally {
			unlock();
		}
		
		return max(0, val);
	}
	
	public long numPinnedBytes() {
		long val = activeBytesHuge.sum();
		for (int i = 0; i < chunkListMetrics.size(); i++) {
			for (PoolChunkMetric m : chunkListMetrics.get(i)) {
				val += ((PoolChunk<?>) m).pinnedBytes();	
			}
		}
		return max(0, val);
	}
	
	protected abstract PoolChunk<T> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize);
	protected abstract PoolChunk<T> newUnpooledChunk(int capacity);
	protected abstract PooledByteBuf<T> newByteBuf(int maxCapacity);
	protected abstract void memoryCopy(T src, int srcOffset, PooledByteBuf<T> dst, int length);
	protected abstract void destroyChunk(PoolChunk<T> chunk);
	
	@Override
	public String toString() {
		lock();
		try {
			StringBuilder buf = new StringBuilder()
                    .append("Chunk(s) at 0~25%:")
                    .append(StringUtil.NEWLINE)
                    .append(qInit)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 0~50%:")
                    .append(StringUtil.NEWLINE)
                    .append(q000)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 25~75%:")
                    .append(StringUtil.NEWLINE)
                    .append(q025)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 50~100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q050)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 75~100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q075)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q100)
                    .append(StringUtil.NEWLINE)
                    .append("small subpages:");
            appendPoolSubPages(buf, smallSubpagePools);
            buf.append(StringUtil.NEWLINE);
            return buf.toString();
		} finally {
			unlock();
		}
	}
	
	private static void appendPoolSubPages(StringBuilder buf, PoolSubpage<?>[] subpages) {
		for (int i = 0; i < subpages.length; i++) {
			PoolSubpage<?> head = subpages[i];
			if (head.next == head || head.next == null) {
				continue;
			}
			
			buf.append(StringUtil.NEWLINE)
			.append(i).append(": ");
			PoolSubpage<?> s = head.next;
			while (s != null) {
				buf.append(s);
				s = s.next;
				if (s == head) {
					break;
				}
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected final void finalize() throws Throwable {
		try {
			destroyPoolSubPages(smallSubpagePools);
			destroyPoolChunkLists(qInit, q000, q025, q050, q075, q100);
		} finally {
			super.finalize();
		}
	}
	
	private static void destroyPoolSubPages(PoolSubpage<?>[] pages) {
		for (PoolSubpage<?> page : pages) {
			page.destroy();
		}
	}
	
	@SafeVarargs
	private void destroyPoolChunkLists(PoolChunkList<T>... chunkLists) {
		for (PoolChunkList<T> chunkList : chunkLists) {
			chunkList.destroy(this);
		}
	}
	
	static final class HeapArena extends PoolArena<byte[]> {
		private final AtomicReference<PoolChunk<byte[]>> lastDestroyedChunk;
		
		HeapArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
			super(parent, sizeClass);
			lastDestroyedChunk = new AtomicReference<>();
		}
		
		private static byte[] newByteArray(int size) {
			return PlatformDependent.allocateUninitializedArray(size);
		}
		
		@Override
		boolean isDirect() {
			return false;
		}
		
		@Override
		protected PoolChunk<byte[]> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize) {
			PoolChunk<byte[]> chunk = lastDestroyedChunk.getAndSet(null);
			if (chunk != null) {
				assert chunk.chunkSize == chunkSize &&
						chunk.pageSize == pageSize &&
						chunk.maxPageIdx == maxPageIdx &&
						chunk.pageShifts == pageShifts;
				return chunk;
			}
			return new PoolChunk<byte[]> (
					this, null, null, newByteArray(chunkSize), pageSize, pageShifts, chunkSize, maxPageIdx);
		}
		
		@Override
		protected PoolChunk<byte[]> newUnpooledChunk(int capacity) {
			return new PoolChunk<byte[]>(this, null, null, newByteArray(capacity), capacity);
		}
		
		@Override
		protected void destroyChunk(PoolChunk<byte[]> chunk) {
			PooledByteBufAllocator.onDeallocateChunk(chunk, !chunk.unpooled);
			if (!chunk.unpooled && lastDestroyedChunk.get() == null) {
				lastDestroyedChunk.set(chunk);
			}
		}
		
		@Override
		protected PooledByteBuf<byte[]> newByteBuf(int maxCapacity) {
			return HAS_UNSAFE ? PooledUnsafeHeapByteBuf.newUnsafeInstance(maxCapacity)
					: PooledHeapByteBuf.newInstance(maxCapacity);
		}
		
		@Override
		protected void memoryCopy(byte[] src, int srcOffset, PooledByteBuf<byte[]> dst, int length) {
			if (length == 0) {
				return;
			}
			
			System.arraycopy(src, srcOffset, dst.memory, dst.offset, length);
		}
	}
	
	static final class DirectArena extends PoolArena<ByteBuffer> {
		DirectArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
			super(parent, sizeClass);
		}
		
		@Override
		boolean isDirect() {
			return true;
		}
		
		@Override
		protected PoolChunk<ByteBuffer> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize) {
			if (sizeClass.directMemoryCacheAlignment == 0) {
				CleanableDirectBuffer cleanableDirectBuffer = allocateDirect(chunkSize);
				ByteBuffer memory = cleanableDirectBuffer.buffer();
				return new PoolChunk<ByteBuffer>(this, cleanableDirectBuffer, memory, memory, pageSize, pageShifts,
						chunkSize, maxPageIdx);
			}
			
			CleanableDirectBuffer cleanableDirectBuffer = allocateDirect(
					chunkSize + sizeClass.directMemoryCacheAlignment);
			final ByteBuffer base = cleanableDirectBuffer.buffer();
			final ByteBuffer memory = PlatformDependent.alignDirectBuffer(base, sizeClass.directMemoryCacheAlignment);
			return new PoolChunk<ByteBuffer>(this, cleanableDirectBuffer, base, memory, pageSize,
					pageShifts, chunkSize, maxPageIdx);
		}
		
		@Override
		protected PoolChunk<ByteBuffer> newUnpooledChunk(int capacity) {
			if (sizeClass.directMemoryCacheAlignment == 0) {
				CleanableDirectBuffer cleanableDirectBuffer = allocateDirect(capacity);
				ByteBuffer memory = cleanableDirectBuffer.buffer();
				return new PoolChunk<ByteBuffer>(this, cleanableDirectBuffer, memory, memory, capacity);
			}
			
			CleanableDirectBuffer cleanableDirectBuffer = allocateDirect(
					capacity + sizeClass.directMemoryCacheAlignment);
			final ByteBuffer base = cleanableDirectBuffer.buffer();
			final ByteBuffer memory = PlatformDependent.alignDirectBuffer(base, sizeClass.directMemoryCacheAlignment);
			return new PoolChunk<ByteBuffer>(this, cleanableDirectBuffer, base, memory, capacity);
		}
		
		private static CleanableDirectBuffer allocateDirect(int capacity) {
			return PlatformDependent.allocateDirect(capacity, true);
		}
		
		@Override
		protected void destroyChunk(PoolChunk<ByteBuffer> chunk) {
			PooledByteBufAllocator.onDeallocateChunk(chunk, !chunk.unpooled);
			chunk.cleanable.clean();
		}
		
		@Override
		protected PooledByteBuf<ByteBuffer> newByteBuf(int maxCapacity) {
			if (HAS_UNSAFE) {
				return PooledUnsafeDirectByteBuf.newInstance(maxCapacity);
			} else {
				return PooledDirectByteBuf.newInstance(maxCapacity);
			}
		}
		
		@Override
		protected void memoryCopy(ByteBuffer src, int srcOffset, PooledByteBuf<ByteBuffer> dstBuf, int length) {
			if (length == 0) {
				return;
			}
			
			if (HAS_UNSAFE) {
				PlatformDependent.copyMemory(
						PlatformDependent.directBufferAddress(src) + srcOffset,
						PlatformDependent.directBufferAddress(dstBuf.memory) + dstBuf.offset, length);
			} else {
				src = src.duplicate();
				ByteBuffer dst = dstBuf.internalNioBuffer();
				src.position(srcOffset).limit(srcOffset + length);
				dst.position(dstBuf.offset);
				dst.put(src);
			}
		}
	}
	
	void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    @Override
    public int sizeIdx2size(int sizeIdx) {
        return sizeClass.sizeIdx2size(sizeIdx);
    }

    @Override
    public int sizeIdx2sizeCompute(int sizeIdx) {
        return sizeClass.sizeIdx2sizeCompute(sizeIdx);
    }

    @Override
    public long pageIdx2size(int pageIdx) {
        return sizeClass.pageIdx2size(pageIdx);
    }

    @Override
    public long pageIdx2sizeCompute(int pageIdx) {
        return sizeClass.pageIdx2sizeCompute(pageIdx);
    }

    @Override
    public int size2SizeIdx(int size) {
        return sizeClass.size2SizeIdx(size);
    }

    @Override
    public int pages2pageIdx(int pages) {
        return sizeClass.pages2pageIdx(pages);
    }

    @Override
    public int pages2pageIdxFloor(int pages) {
        return sizeClass.pages2pageIdxFloor(pages);
    }

    @Override
    public int normalizeSize(int size) {
        return sizeClass.normalizeSize(size);
    }
}
