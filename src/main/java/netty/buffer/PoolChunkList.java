package netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import netty.common.util.internal.StringUtil;

import static java.lang.Math.*;


public class PoolChunkList<T> implements PoolChunkListMetric {
	private static final Iterator<PoolChunkMetric> EMPTY_METRICS = Collections.emptyIterator();
	private final PoolArena<T> arena;
	private final PoolChunkList<T> nextList;
	private final int minUsage;
	private final int maxUsage;
	private final int maxCapacity;
	private PoolChunk<T> head;
	private final int freeMinThreshold;
	private final int freeMaxThreshold;
	
	private PoolChunkList<T> prevList;
	
	PoolChunkList(PoolArena<T> arena, PoolChunkList<T> nextList, int minUsage, int maxUsage, int chunkSize) {
		assert minUsage <= maxUsage;
		this.arena = arena;
		this.nextList = nextList;
		this.minUsage = minUsage;
		this.maxUsage = maxUsage;
		maxCapacity = calculateMaxCapacity(minUsage, chunkSize);
		
		freeMinThreshold = (maxUsage == 100) ? 0 : (int) (chunkSize * (100.0 - maxUsage + 0.99999999) / 100L);
		freeMaxThreshold = (minUsage == 100) ? 0 : (int) (chunkSize * (100.0 - minUsage + 0.99999999) / 100L);
	}
	
	private static int calculateMaxCapacity(int minUsage, int chunkSize) {
		minUsage = minUsage0(minUsage);
		
		if (minUsage == 100) {
			return 0;
		}
		
		return (int) (chunkSize * (100L - minUsage) / 100L);
	}
	
	void prevList(PoolChunkList<T> prevList) {
		assert this.prevList == null;
		this.prevList = prevList;
	}
	
	boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
		int normCapacity = arena.sizeClass.sizeIdx2size(sizeIdx);
		if (normCapacity > maxCapacity) {
			return false;
		}
		
		for (PoolChunk<T> cur = head; cur != null; cur = cur.next) {
			if (cur.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
				if (cur.freeBytes <= freeMinThreshold) {
					remove(cur);
					nextList.add(cur);
				}
				return true;
			}
		}
		return false;
	}
	
	boolean free(PoolChunk<T> chunk, long handle, int normCapacity, ByteBuffer nioBuffer) {
		chunk.free(handle, normCapacity, nioBuffer);
		if (chunk.freeBytes > freeMaxThreshold) {
			remove(chunk);
			return remove0(chunk);
		}
		return true;
	}
	
	private boolean move(PoolChunk<T> chunk) {
		assert chunk.usage() < maxUsage;
		
		if (chunk.freeBytes > freeMaxThreshold) {
			return move0(chunk);
		}
		
		add0(chunk);
		return true;
	}
	
	private boolean move0(PoolChunk<T> chunk) {
		if (prevList == null) {
			assert chunk.usage() == 0;
			return false;
		}
		return prevList.move(chunk);
	}
	
	void add(PoolChunk<T> chunk) {
		if (chunk.freeBytes <= freeMinThreshold) {
			nextList.add(chunk);
			return;
		}
		add0(chunk);
	}
	
	void add0(PoolChunk<T> chunk) {
		chunk.parent = this;
		if (head == null) {
			head = chunk;
			chunk.prev = null;
			chunk.next = null;
		} else {
			chunk.prev = null;
			chunk.next = head;
			head.prev = chunk;
			head = chunk;
		}
	}
	
	private void remove(PoolChunk<T> cur) {
		if (cur == head) {
			head = cur.next;
			if (head != null) {
				head.prev = null;
			}
		} else {
			PoolChunk<T> next = cur.next;
			cur.prev.next = next;
			if (next != null) {
				next.prev = cur.prev;
			}
		}
	}
	
	@Override
	public int minUsage() {
		return minUsage0(minUsage);
	}
	
	@Override
	public int maxUsage() {
		return min(maxUsage, 100);
	}
	
	private static int minUsage0(int value) {
		return max(1, value);
	}
	
	@Override
	public Iterator<PoolChunkMetric> iterator() {
		arena.lock();
		try {
			if (head == null) {
				return EMPTY_METRICS;
			}
			List<PoolChunkMetric> metrics = new ArrayList<PoolChunkMetric>();
			for (PoolChunk<T> cur = head;;) {
				metrics.add(cur);
				cur = cur.next;
				if (cur == null) {
					break;
				}
			}
			return metrics.iterator();
		} finally {
			arena.unlock();
		}
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		arena.lock();
		try {
			if (head == null) {
				return "none";
			}
			
			for (PoolChunk<T> cur = head;;) {
				buf.append(cur);
				cur = cur.next;
				if (cur == null) {
					break;
				}
				buf.append(StringUtil.NEWLINE);
			}
		} finally {
			arena.unlock();
		}
		return buf.toString();
	}
	
	void destroy(PoolArena<T> arena) {
		PoolChunk<T> chunk = head;
		while (chunk != null) {
			arena.destroyChunk(chunk);
			chunk = chunk.next;
		}
		head = null;
	}
}
