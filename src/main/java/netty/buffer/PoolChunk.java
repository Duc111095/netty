package netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import netty.common.util.internal.CleanableDirectBuffer;
import netty.common.util.internal.LongLongHashMap;
import netty.common.util.internal.SystemPropertyUtil;

final class PoolChunk<T> implements PoolChunkMetric, ChunkInfo {
	private static final int SIZE_BIT_LENGTH = 15;
	private static final int INUSED_BIT_LENGTH = 1;
	private static final int SUBPAGE_BIT_LENGTH = 1;
	private static final int BITMAP_IDX_BIT_LENGTH = 32;
	
	private static final boolean trackPinnedMemory = 
			SystemPropertyUtil.getBoolean("io.netty.trackPinnedMemory", true);
	
	static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH;
	static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT;
	static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT;
	static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT;
	
	final PoolArena<T> arena;
	final CleanableDirectBuffer cleanable;
	final Object base;
	final T memory;
	final boolean unpooled;
	
	private final LongLongHashMap runsAvailMap;
	private final IntPriorityQueue[] runsAvail;
	private final ReentrantLock runsAvailLock;
	private final PoolSubpage<T>[] subpages;
	
	private final LongAdder pinnedBytes;
	
	final int pageSize;
	final int pageShifts;
	final int chunkSize;
	final int maxPageIdx;
	
	private final Deque<ByteBuffer> cachedNioBuffers;
	int freeBytes;
	
	PoolChunkList<T> parent;
	PoolChunk<T> prev;
	PoolChunk<T> next;
	
	PoolChunk(PoolArena<T> arena, CleanableDirectBuffer cleanable, Object base, T memory, int pageSize, int pageShifts,
			int chunkSize, int maxPageIdx) {
		unpooled = false;
		this.arena = arena;
		this.cleanable = cleanable;
		this.base = base;
		this.memory = memory;
		this.pageSize = pageSize;
		this.pageShifts = pageShifts;
		this.chunkSize = chunkSize;
		this.maxPageIdx = maxPageIdx;
		freeBytes = chunkSize;
		
		runsAvail = newRunsAvailqueueArray(maxPageIdx);
		runsAvailLock = new ReentrantLock();
		runsAvailMap = new LongLongHashMap(-1);
		subpages = new PoolSubpage[chunkSize >> pageShifts];
		
		int pages = chunkSize >> pageShifts;
		long initHandle = (long) pages << SIZE_SHIFT;
		insertAvailRun(0, pages, initHandle);
		
		cachedNioBuffers = new ArrayDeque<>(8);
		this.pinnedBytes = trackPinnedMemory ? new LongAdder() : null;
	}
	
	PoolChunk(PoolArena<T> arena, CleanableDirectBuffer cleanable, Object base, T memory, int size) {
		unpooled = true;
		this.arena = arena;
		this.cleanable = cleanable;
		this.base = base;
		this.memory = memory;
		pageSize = 0;
		pageShifts = 0;
		maxPageIdx = 0;
		runsAvailMap = null;
		runsAvail = null;
		runsAvailLock = null;
		subpages = null;
		chunkSize = size;
		cachedNioBuffers = null;
		this.pinnedBytes = trackPinnedMemory ? new LongAdder() : null;
	}
	
	private static IntPriorityQueue[] newRunsAvailqueueArray(int size) {
		IntPriorityQueue[] queueArray = new IntPriorityQueue[size];
		for (int i = 0; i < queueArray.length; i++) {
			queueArray[i] = new IntPriorityQueue();
		}
		return queueArray;
	}
	
	private void insertAvailRun(int runOffset, int pages, long handle) {
		int pageIdxFloor = arena.sizeClass.pages2pageIdxFloor(pages);
		IntPriorityQueue queue = runsAvail[pageIdxFloor];
		assert isRun(handle);
		queue.offer((int) (handle >> BITMAP_IDX_BIT_LENGTH));
		
		insertAvailRun0(runOffset, handle);
		if (pages > 1) {
			insertAvailRun0(lastPage(runOffset, pages), handle);
		}
	}
	
	private void insertAvailRun0(int runOffset, long handle) {
		long pre = runsAvailMap.put(runOffset, handle);
		assert pre == -1;
	}
	
	private void removeAvailRun(long handle) {
		int pageIdxFloor = arena.sizeClass.pages2pageIdxFloor(runPages(handle));
		runsAvail[pageIdxFloor].remove((int) (handle >> BITMAP_IDX_BIT_LENGTH));
		removeAvailRun0(handle);
	}
	
	private void removeAvailRun0(long handle) {
		int runOffset = runOffset(handle);
		int pages = runPages(handle);
		
		runsAvailMap.remove(runOffset);
		if (pages > 1) {
			runsAvailMap.remove(lastPage(runOffset, pages));
		}
	}
	
	private static int lastPage(int runOffset, int pages) {
		return runOffset + pages - 1;
	}
	
	private long getAvailRunByOffset(int runOffset) {
		return runsAvailMap.get(runOffset);
	}
	
	@Override
	public int usage() {
		final int freeBytes;
		if (this.unpooled) {
			freeBytes = this.freeBytes;
		} else {
			runsAvailLock.lock();
			try {
				freeBytes = this.freeBytes;
			} finally {
				runsAvailLock.unlock();
			}
		}
		return usage(freeBytes);
	}
	
	private int usage(int freeBytes) {
		if (freeBytes == 0) {
			return 100;
		}
		
		int freePercentage = (int) (freeBytes * 100L / chunkSize);
		if (freePercentage == 0) {
			return 90;
		}
		return 100 - freePercentage;
	}
	
	boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
		final long handle;
		if (sizeIdx <= arena.sizeClass.smallMaxSizeIdx) {
			final PoolSubpage<T> nextSub;
			
			PoolSubpage<T> head = arena.smallSubpagePools[sizeIdx];
			head.lock();
			try {
				nextSub = head.next;
				if (nextSub != head) {
					assert nextSub.doNotDestroy && nextSub.elemSize == arena.sizeClass.sizeIdx2size(sizeIdx) :
						"doNotDestroy=" + nextSub.doNotDestroy + ", elemSize=" + nextSub.elemSize + ", sizeIdx=" + 
							sizeIdx;
					handle = nextSub.allocate();
					assert handle >= 0;
					assert isSubpage(handle);
					nextSub.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache, false);
					return true;
				}
				handle = allocateSubpage(sizeIdx, head);
				if (handle < 0) {
					return false;
				}
				assert isSubpage(handle);
			} finally {
				head.unlock();
			}
		} else {
			int runSize = arena.sizeClass.sizeIdx2size(sizeIdx);
			handle = allocateRun(runSize);
			if (handle < 0) {
				return false;
			}
			assert !isSubpage(handle);
		}
		
		ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
		initBuf(buf, nioBuffer, handle, reqCapacity, cache, false);
		return true;
	}
	
	private long allocateRun(int runSize) {
		int pages = runSize >> pageShifts;
		int pageIdx = arena.sizeClass.pages2pageIdx(pages);
		
		runsAvailLock.lock();
		try {
			int queueIdx = runFirstBestFit(pageIdx);
			if (queueIdx == -1) {
				return -1;
			}
			
			IntPriorityQueue queue = runsAvail[queueIdx];
			long handle = queue.poll();
			assert handle != IntPriorityQueue.NO_VALUE;
			handle <<= BITMAP_IDX_BIT_LENGTH;
			assert !isUsed(handle) : "invalid handle: " + handle;
			
			removeAvailRun0(handle);
			
			handle = splitLargeRun(handle, pages);
			int pinnedSize = runSize(pageShifts, handle);
			freeBytes -= pinnedBytes;
			return handle;
		} finally {
			runsAvailLock.unlock();
		}
	}
	
	private int calculateRunSize(int sizeIdx) {
		int maxElements = 1 << pageShifts - SizeClasses.LOG2_QUANTUM;
		int runSize = 0;
		int nElements;
		
		final int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);
		
		do {
			runSize += pageSize;
			nElements = runSize / elemSize;
		} while (nElements < maxElements && runSize != nElements * elemSize);
		
		while (nElements > maxElements) {
			runSize -= pageSize;
			nElements = runSize / elemSize;
		}
		
		assert nElements > 0;
		assert runSize <= chunkSize;
		assert runSize >= elemSize;
		
		return runSize;
	}	

	private int runFirstBestFit(int pageIdx) {
		if (freeBytes == chunkSize) {
			return arena.sizeClass.nPSizes - 1;
		}
		for (int i = pageIdx; i < arena.sizeClass.nPSizes; i++) {
			IntPriorityQueue queue = runsAvail[i];
			if (queue != null && !queue.isEmpty()) {
				return i;
			}
		}
		return -1;
	}
	
	private long splitLargeRun(long handle, int needPages) {
		assert needPages > 0;
		
		int totalPages = runPages(handle);
		assert needPages <= totalPages;
		
		int remPages = totalPages - needPages;
		
		if (remPages > 0) {
			int runOffset = runOffset(handle);
			
			int availOffset = runOffset + needPages;
			long availRun = toRunHandle(availOffset, remPages, 0);
			insertAvailRun(availOffset, remPages, availRun);
			
			return toRunHandle(runOffset, needPages, 1);
		}
		
		handle |= 1L << IS_USED_SHIFT;
		return handle;
	}
	
	private long allocateSubpage(int sizeIdx, PoolSubpage<T> head) {
		int runSize = calculateRunSize(sizeIdx);
		long runHandle = allocateRun(runSize);
		if (runHandle < 0) {
			return -1;
		}
		
		int runOffset = runOffset(runHandle);
		assert subpages[runOffset] == null;
		int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);
		
		PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
				runSize(pageShifts, runHandle), elemSize);
		
		subpages[runOffset] = subpage;
		return subpage.allocate();
	}
	
	void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
		if (isSubpage(handle)) {
			int sIdx = runOffset(handle);
			PoolSubpage<T> subpage = subpages[sIdx];
			assert subpage != null;
			PoolSubpage<T> head = subpage.chunk.arena.smallSubpagePools[subpage.headIndex];
			
			head.lock();
			try {
				assert subpage.doNotDestroy;
				if (subpage.free(head, bitmapIdx(handle))) {
					return;
				}
				assert !subpage.doNotDestroy;
				subpages[sIdx] = null;
			} finally {
				head.unlock();
			}
		}
		
		int runSize = runSize(pageShifts, handle);
		
		runsAvailLock.lock();
		try {
			long finalRun = collapseRuns(handle);
			
			//set run as not used
			finalRun &= ~(1L << IS_USED_SHIFT);
			//if it is a subpage, set it to run
			finalRun &= ~(1L << IS_SUBPAGE_SHIFT);
			
			insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
			freeBytes += runSize;
		} finally {
			runsAvailLock.unlock();
		}
		
		if (nioBuffer != null && cachedNioBuffers != null && 
				cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
			cachedNioBuffers.offer(nioBuffer);
		}
	}
	
	private long collapseRuns(long handle) {
		return collapseNext(collapsePast(handle));
	}
	
	private long collapsePast(long handle) {
		for (;;) {
			int runOffset = runOffset(handle);
			int runPages = runPages(handle);
			
			long pastRun = getAvailRunByOffset(runOffset - 1);
			if (pastRun == -1) {
				return handle;
			}
			
			int pastOffset = runOffset(pastRun);
			int pastPages = runPages(pastRun);
			
			if (pastRun != handle && pastOffset + pastPages == runOffset) {
				removeAvailRun(pastRun);
				handle = toRunHandle(pastOffset, pastPages + runPages, 0);
			} else {
				return handle;
			}
		}
	}
	
	private long collapseNext(long handle) {
		for (;;) {
			int runOffset = runOffset(handle);
			int runPages = runPages(handle);
			
			long nextRun = getAvailRunByOffset(runOffset + runPages);
			if (nextRun == -1) {
				return handle;
			}
			
			int nextOffset = runOffset(nextRun);
			int nextPages = runPages(nextRun);
			
			if (nextRun != handle && runOffset + runPages == nextOffset) {
				removeAvailRun(nextRun);
				handle = toRunHandle(runOffset, runPages + nextPages, 0);
			} else {
				return handle;
			}
		}
	}
	
	private static long toRunHandle(int runOffset, int runPages, int inUsed) {
		return (long) runOffset << RUN_OFFSET_SHIFT 
				| (long) runPages << SIZE_SHIFT
				| (long) inUsed << IS_USED_SHIFT;
	}
	
	void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
			PoolThreadCache threadCache, boolean threadLocal) {
		if (isSubpage(handle)) {
			initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache, threadLocal);
		} else {
			int maxLength = runSize(pageShifts, handle);
			buf.init(this, nioBuffer, handle, runOffset(handle) << pageShifts,
					reqCapacity, maxLength, arena.parent.threadCache(), threadLocal);
		}
	}
	
	void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
			PoolThreadCache threadCache, boolean threadLocal) {
		int runOffset = runOffset(handle);
		int bitmapIdx = bitmapIdx(handle);
		
		PoolSubpage<T> s = subpages[runOffset];
		assert s.isDoNotDestroy();
		assert reqCapacity <= s.elemSize : reqCapacity + "<=" + s.elemSize;
		
		int offset = (runOffset << pageShifts) + bitmapIdx * s.elemSize;
		buf.init(this, nioBuffer, handle, offset, reqCapacity, s.elemSize, threadCache, threadLocal);
	}
	
	void incrementPinnedMemory(int delta) {
		assert delta > 0;
		if (pinnedBytes != null) {
			pinnedBytes.add(delta);
		}
	}
	
	void decrementPinnedMemory(int delta) {
		assert delta > 0;
		if (pinnedBytes != null) {
			pinnedBytes.add(-delta);
		}
	}
	
	@Override
	public int chunkSize() {
		return chunkSize;
	}
	
	@Override
	public int freeBytes() {
		if (this.unpooled) {
			return freeBytes;
		}
		runsAvailLock.lock();
		try {
			return freeBytes;
		} finally {
			runsAvailLock.unlock();
		}
	}
	
	public int pinnedBytes() {
		return pinnedBytes == null ? 0 : (int) pinnedBytes.sum();
	}
	
	@Override
	public String toString() {
		final int freeBytes;
		if (this.unpooled) {
			freeBytes = this.freeBytes;
		} else {
			runsAvailLock.lock();
			try {
				freeBytes = this.freeBytes;
			} finally {
				runsAvailLock.unlock();
			}
		}
		
		return new StringBuilder()
				.append("Chunk(")
				.append(Integer.toHexString(System.identityHashCode(this)))
				.append(": ")
				.append(usage(freeBytes))
				.append("%, ")
				.append(chunkSize - freeBytes)
				.append('/')
				.append(chunkSize)
				.append(')')
				.toString();
	}
	
	void destroy() {
		arena.destroyChunk(this);
	}
	
	static int runOffset(long handle) {
		return (int) (handle >> RUN_OFFSET_SHIFT);
	}
	
	static int runSize(int pageShifts, long handle) {
		return runPages(handle) << pageShifts;
	}
	
	static int runPages(long handle) {
		return (int) (handle >> SIZE_SHIFT & 0x7fff);
	}
	
	static boolean isUsed(long handle) {
		return (int) (handle >> IS_USED_SHIFT & 1) == 1L;
	}
	
	static boolean isRun(long handle) {
		return !isSubpage(handle);
	}
	
	static boolean isSubpage(long handle) {
		return (handle >> IS_SUBPAGE_SHIFT & 1) == 1L;
	}
	
	static int bitmapIdx(long handle) {
		return (int) handle;
	}
	
	@Override
	public int capacity() {
		return chunkSize;
	}
	
	@Override
	public boolean isDirect() {
		return cleanable != null && cleanable.buffer().isDirect();
	}
	
	@Override
	public long memoryAddress() {
		return cleanable != null && cleanable.hasMemoryAddress() ? cleanable.memoryAddress() : 0L;
	}
}
