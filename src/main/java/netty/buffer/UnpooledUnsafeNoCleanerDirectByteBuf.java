package netty.buffer;

import java.nio.ByteBuffer;

import netty.common.util.internal.CleanableDirectBuffer;
import netty.common.util.internal.PlatformDependent;

class UnpooledUnsafeNoCleanerDirectByteBuf extends UnpooledUnsafeDirectByteBuf {

	UnpooledUnsafeNoCleanerDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
		super(alloc, initialCapacity, maxCapacity);
	}
	
	@Override
	protected ByteBuffer allocateDirect(int initialCapacity) {
		throw new UnsupportedOperationException();
	}
	
	CleanableDirectBuffer reallocateDirect(CleanableDirectBuffer oldBuffer, int newCapacity) {
		return PlatformDependent.reallocateDirect(oldBuffer, newCapacity);
	}
	
	@Override
	protected void freeDirect(ByteBuffer buffer) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ByteBuf capacity(int newCapacity) {
		checkNewCapacity(newCapacity);
		
		int oldCapacity = capacity();
		if (newCapacity == oldCapacity) {
			return this;
		}
		
		trimIndiciesToCapacity(newCapacity);
		setByteBuffer(reallocateDirect(cleanable, newCapacity), false);
		return this;
	}
}
