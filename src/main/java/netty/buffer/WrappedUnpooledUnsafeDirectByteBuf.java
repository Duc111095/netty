package netty.buffer;

import java.nio.ByteBuffer;

import netty.common.util.internal.PlatformDependent;

final class WrappedUnpooledUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
	WrappedUnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, long memoryAddres, int size, boolean doFree) {
		super(alloc, PlatformDependent.directBuffer(memoryAddres, size), size, doFree);
	}
	
	@Override
	protected void freeDirect(ByteBuffer buffer) {
		PlatformDependent.freeMemory(memoryAddress);
	}
}
