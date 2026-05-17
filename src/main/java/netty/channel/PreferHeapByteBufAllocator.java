package netty.channel;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.buffer.CompositeByteBuf;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.UnstableApi;

@UnstableApi
public final class PreferHeapByteBufAllocator implements ByteBufAllocator {
	private final ByteBufAllocator allocator;
	
	public PreferHeapByteBufAllocator(ByteBufAllocator allocator) {
		this.allocator = ObjectUtil.checkNotNull(allocator, "allocator");
	}
	
	@Override
	public ByteBuf buffer() {
		return allocator.heapBuffer();
	}
	
	@Override
	public ByteBuf buffer(int initialCapacity) {
		return allocator.heapBuffer(initialCapacity);
	}
	
	@Override
	public ByteBuf buffer(int initialCapacity, int maxCapacity) {
		return allocator.heapBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
	public ByteBuf ioBuffer() {
		return allocator.heapBuffer();
	}
	
	@Override
	public ByteBuf ioBuffer(int initialCapacity) {
		return allocator.heapBuffer(initialCapacity);
	}
	
	@Override
	public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
		return allocator.heapBuffer(initialCapacity, maxCapacity);
	}
	
	@Override
    public ByteBuf heapBuffer() {
        return allocator.heapBuffer();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return allocator.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return allocator.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf directBuffer() {
        return allocator.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return allocator.directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return allocator.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return allocator.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return allocator.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return allocator.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return allocator.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return allocator.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return allocator.compositeDirectBuffer(maxNumComponents);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return allocator.isDirectBufferPooled();
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return allocator.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
}
