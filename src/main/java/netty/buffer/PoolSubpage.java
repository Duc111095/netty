package netty.buffer;

import java.util.concurrent.locks.ReentrantLock;

import static netty.buffer.PoolChunk.RUN_OFFSET_SHIFT;
import static netty.buffer.PoolChunk.SIZE_SHIFT;
import static netty.buffer.PoolChunk.IS_USED_SHIFT;
import static netty.buffer.PoolChunk.IS_SUBPAGE_SHIFT;


final class PoolSubpage<T> implements PoolSubpageMetric {
	final PoolChunk<T> chunk;
	final int elemSize;
	private final int pageShifts;
	private final int runOffset;
	private final int runSize;
	private final long[] bitmap;
	private final int bitmapLength;
	private final int maxNumElems;
	final int headIndex;
	
	PoolSubpage<T> prev;
	PoolSubpage<T> next;
	
	boolean doNotDestroy;
	private int nextAvail;
	private int numAvail;
	
	final ReentrantLock lock;
	
	PoolSubpage(int headIndex) {
		chunk = null;
		lock = new ReentrantLock();
		pageShifts = -1;
		runOffset = -1;
		elemSize = -1;
		runSize = -1;
		bitmap = null;
		bitmapLength = -1;
		maxNumElems = 0;
		this.headIndex = headIndex;
	}
	
	PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int pageShifts, int runOffset, int runSize, int elemSize) {
		this.headIndex = head.headIndex;
		this.chunk = chunk;
		this.pageShifts = pageShifts;
		this.runOffset = runOffset;
		this.runSize = runSize;
		this.elemSize = elemSize;
		
		doNotDestroy = true;
		
		maxNumElems = numAvail = runSize / elemSize;
		int bitmapLength = maxNumElems >>> 6;
		if ((maxNumElems & 63) != 0) {
			bitmapLength++;
		}
		this.bitmapLength = bitmapLength;
		bitmap = new long[bitmapLength];
		nextAvail = 0;
		
		lock = null;
		addToPool(head);
	}
	
	long allocate() {
		if (numAvail == 0 || !doNotDestroy) {
			return -1;
		}
		
		final int bitmapIdx = getNextAvail();
		if (bitmapIdx < 0) {
			removeFromPool();
			throw new AssertionError("No next available bitmap index found (bitmapIdx = " + bitmapIdx + "), " +
					"even though there are supposed to be (numAvail=" + numAvail + ") " + 
					"out of (maxNumElems = " + maxNumElems + ") available indexes.");
		}
		int q = bitmapIdx >>> 6;
		int r = bitmapIdx & 63;
		assert (bitmap[q] >>> r & 1) == 0;
		bitmap[q] |= 1L << r;
		
		if (-- numAvail == 0) {
			removeFromPool();
		}
		
		return toHandle(bitmapIdx);
	}
	
	boolean free(PoolSubpage<T> head, int bitmapIdx) {
		int q = bitmapIdx >>> 6;
		int r = bitmapIdx & 63;
		assert (bitmap[q] >>> r & 1) != 0;
		bitmap[q] ^= 1L << r;
		
		setNextAvail(bitmapIdx);
		
		if (numAvail ++ == 0) {
			addToPool(head);
			if (maxNumElems > 1) {
				return true;
			}
		}
		
		if (numAvail != maxNumElems) {
			return true;
		} else {
			if (prev == next) {
				return true;
			}
			doNotDestroy = false;
			removeFromPool();
			return false;
		}
	}
	
	private void addToPool(PoolSubpage<T> head) {
		assert prev == null && next == null;
		prev = head;
		next = head.next;
		next.prev = this;
		head.next = this;
	}
	
	private void removeFromPool() {
		assert prev != null && next != null;
		prev.next = next;
		next.prev = prev;
		next = null;
		prev = null;
	}
	
	private void setNextAvail(int bitmapIdx) {
		nextAvail = bitmapIdx;
	}
	
	private int getNextAvail() {
		int nextAvail = this.nextAvail;
		if (nextAvail >= 0) {
			this.nextAvail = -1;
			return nextAvail;
		}
		return findNextAvail();
	}
	
	private int findNextAvail() {
		for (int i = 0; i < bitmapLength; i++) {
			long bits = bitmap[i];
			if (~bits != 0) {
				return findNextAvail0(i, bits);
			}
		}
		return -1;
	}
	
	private int findNextAvail0(int i, long bits) {
		final int baseVal = i << 6;
		for (int j = 0; j < 64; j++) {
			if ((bits & 1) == 0) {
				int val = baseVal | j;
				if (val < maxNumElems) {
					return val;
				} else {
					break;
				}
			}
			
			bits >>>= 1;
		}
		return -1;
	}
	
	private long toHandle(int bitmapIdx) {
		int pages = runSize >> pageShifts;
		return (long) runOffset << RUN_OFFSET_SHIFT
				| (long) pages << SIZE_SHIFT
				| 1L << IS_USED_SHIFT
				| 1L << IS_SUBPAGE_SHIFT
				| bitmapIdx;
	}
	
	@Override
	public String toString() {
		final int numAvail;
		if (chunk == null) {
			numAvail = 0;
		} else {
			final boolean doNotDestroy;
			PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
			head.lock();
			try {
				doNotDestroy = this.doNotDestroy;
				numAvail = this.numAvail;
			} finally {
				head.unlock();
			}
			if (!doNotDestroy) {
				return "(" + runOffset + ": not in use)";
			}
		}
		
		return "(" + this.runOffset + ": " + (this.maxNumElems - numAvail) + '/' + this.maxNumElems + 
				", offset: " + this.runOffset + ", length: " + this.runSize + ", elemSize: " + this.elemSize + ')';
	}
	
	@Override
	public int maxNumElements() {
		return maxNumElems;
	}
	
	@Override
	public int numAvailable() {
		if (chunk == null) {
			return 0;
		}
		PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
		head.lock();
		try {
			return numAvail;
		} finally {
			head.unlock();
		}
	}
	
	@Override
	public int elementSize() {
		return elemSize;
	}
	
	@Override
	public int pageSize() {
		return 1 << pageShifts;
	}
	
	boolean isDoNotDestroy() {
		if (chunk == null) {
			return true;
		}
		PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
		head.lock();
		try {
			return doNotDestroy;
		} finally {
			head.unlock();
		}
	}
	
	void destroy() {
		if (chunk != null) {
			chunk.destroy();
		}
	}
	
	void lock() {
		lock.lock();
	}
	
	void unlock() {
		lock.unlock();
	}
}
