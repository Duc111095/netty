package netty.common.util.internal;

import java.nio.ByteBuffer;

interface Cleaner {
	CleanableDirectBuffer allocate(int capacity);
	
	default CleanableDirectBuffer reallocate(CleanableDirectBuffer old, int newCapacity) {
		CleanableDirectBuffer newBuf = allocate(newCapacity);
		ByteBuffer oldBB = old.buffer();
		ByteBuffer newBB = newBuf.buffer();
		int bytesToCopy = Math.min(oldBB.capacity(), newCapacity);
		oldBB.position(0).limit(bytesToCopy);
		newBB.position(0).limit(bytesToCopy);
		newBB.put(oldBB).clear();
		old.clean();
		return newBuf;
	}
	
	void freeDirectBuffer(ByteBuffer buffer);
	
	boolean hasExpensiveClean();
}
