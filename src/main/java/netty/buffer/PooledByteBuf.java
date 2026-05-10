package netty.buffer;

import java.nio.ByteBuffer;

import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.internal.ObjectPool.Handle;

abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {
	
	private final EnhancedHandle<PooledByteBuf<T>> recyclerHandle;
	
	protected PoolChunk<T> chunk;
	protected long handle;
	protected T memory;
	protected int offset;
	protected int length;
	int maxLength;
	PoolThreadCache cache;
	ByteBuffer tmpNioBuf;
	private ByteBufAllocator allocator;
	
	@SuppressWarnings("unchecked")
	protected PooledByteBuf(Handle<? extends PooledByteBuf<T>> recyclerHandle, int maxCapacity) {
		super(maxCapacity);
		this.recyclerHandle = (EnhancedHandle<PooledByteBuf<T>>) recyclerHandle;
	}
	
	void init(PoolChunk<T> chunk, ByteBuffer nioBuffer,
			long handle, int offset, int length, int maxLength, PoolThreadCache cache, boolean threadLocal) {
		init0(chunk, nioBuffer, handle, offset, length, maxLength, cache, true, threadLocal);
	}
	
	void initUnpooled(PoolChunk<T> chunk, int length) {
		init0(chunk, null, 0, 0, length, length, null, false,
				false);
	}
	
	private void init0(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int offset, int length, int maxLength,
			PoolThreadCache cache, boolean pooled, boolean threadLocal) {
		assert handle >= 0;
		assert chunk != null;
		assert !PoolChunk.isSubpage(handle) ||
			chunk.arena.sizeClass.size2SizeIdx(maxLength) <= chunk.arena.sizeClass.smallMaxSizeIdx :
			"Allocated small sub-page handle for a buffer size that isn't \"small.\"";
		
		chunk.incrementPinnedMemory(maxLength);
		this.chunk = chunk;
		memory = chunk.memory;
		tmpNioBuf = nioBuffer;
		allocator = chunk.arena.parent;
		this.cache = cache;
		this.handle = handle;
		this.offset = offset;
		this.length = length;
		this.maxLength = maxLength;
		PooledByteBufAllocator.onAllocateBuffer(this, pooled, threadLocal);
	}
	
	final void reuse(int maxCapacity) {
		maxCapacity(maxCapacity);
		resetRefCnt();
		setIndex0(0, 0);
		discardMarks();
	}
	
	@Override
	public final int capacity() {
		return length;
	}
	
	@Override
	public int maxFastWritableBytes() {
		return Math.min(maxLength, maxCapacity()) - writerIndex;
	}
	
	@Override
	public final ByteBuf capacity(int newCapacity) {
		if (newCapacity == length) {
			ensureAccessible();
			return this;
		}
		checkNewCapacity(newCapacity);
		if (!chunk.unpooled) {
			if (newCapacity > length) {
				if (newCapacity <= maxLength) {
					length = newCapacity;
					return this;
				}
			} else if (newCapacity > maxLength >>> 1 &&
					(maxLength > 512 || newCapacity > maxLength - 16)) {
				length = newCapacity;
				trimIndicesToCapacity(newCapacity);
				return this;
			}
		}
		
		PooledByteBufAllocator.onReallocateBuffer(this, newCapacity);
		chunk.arena.reallocate(this, newCapacity);
		return this;
	}
	
	@Override
    public final ByteBufAllocator alloc() {
        return allocator;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    @Override
    public final ByteBuf retainedDuplicate() {
        return PooledDuplicatedByteBuf.newInstance(this, this, readerIndex(), writerIndex());
    }

    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }

    @Override
    public final ByteBuf retainedSlice(int index, int length) {
        return PooledSlicedByteBuf.newInstance(this, this, index, length);
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        } else {
            tmpNioBuf.clear();
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            PooledByteBufAllocator.onDeallocateBuffer(this);
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            chunk.arena.free(chunk, tmpNioBuf, handle, maxLength, cache);
            tmpNioBuf = null;
            chunk = null;
            cache = null;
            this.recyclerHandle.unguardedRecycle(this);
        }
    }

    protected final int idx(int index) {
        return offset + index;
    }

    final ByteBuffer _internalNioBuffer(int index, int length, boolean duplicate) {
        index = idx(index);
        ByteBuffer buffer = duplicate ? newInternalNioBuffer(memory) : internalNioBuffer();
        buffer.limit(index + length).position(index);
        return buffer;
    }

    ByteBuffer duplicateInternalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, true);
    }

    @Override
    public final ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, false);
    }

    @Override
    public final int nioBufferCount() {
        return 1;
    }

    @Override
    public final ByteBuffer nioBuffer(int index, int length) {
        return duplicateInternalNioBuffer(index, length).slice();
    }

    @Override
    public final ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public final boolean isContiguous() {
        return true;
    }

    @Override
    public final int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length));
    }

    @Override
    public final int readBytes(GatheringByteChannel out, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false));
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public final int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length), position);
    }

    @Override
    public final int readBytes(FileChannel out, long position, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false), position);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public final int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length));
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public final int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length), position);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }
}
