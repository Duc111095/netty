package netty.buffer;

import java.nio.ByteBuffer;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;

final class ReadOnlyUnsafeDirectByteBuf extends ReadOnlyByteBufferBuf {
	private final long memoryAddress;
	
	ReadOnlyUnsafeDirectByteBuf(ByteBufAllocator allocator, ByteBuffer byteBuffer) {
		super(allocator, byteBuffer);
		
		memoryAddress = PlatformDependent.directBufferAddress(buffer);
	}
	
	@Override
	protected byte _getByte(int index) {
		return UnsafeByteBufUtil.getByte(addr(index));
	}
	
	@Override
    protected short _getShort(int index) {
        return UnsafeByteBufUtil.getShort(addr(index));
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return UnsafeByteBufUtil.getUnsignedMedium(addr(index));
    }

    @Override
    protected int _getInt(int index) {
        return UnsafeByteBufUtil.getInt(addr(index));
    }

    @Override
    protected long _getLong(int index) {
        return UnsafeByteBufUtil.getLong(addr(index));
    }
    
    @Override
    protected ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length, boolean internal) {
    	checkIndex(index, length);
    	ObjectUtil.checkNotNull(dst, "dst");
    	if (dstIndex < 0 || dstIndex > dst.capacity() - length) {
    		throw new IndexOutOfBoundsException("dstIndex: " + dstIndex);
    	}
    	
    	if (dst.hasMemoryAddress()) {
    		PlatformDependent.copyMemory(addr(index), dst.memoryAddress() + dstIndex, length);
    	} else if (dst.hasArray()) {
    		PlatformDependent.copyMemory(addr(index), dst.array(), dst.arrayOffset() + dstIndex, length);
    	} else {
    		dst.setBytes(dstIndex, this, index, length);
    	}
    	return this;
    }
    
    @Override
    protected ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length, boolean internal) {
    	checkIndex(index, length);
    	ObjectUtil.checkNotNull(dst, "dst");
    	if (dstIndex < 0 || dstIndex > dst.length - length) {
    		throw new IndexOutOfBoundsException(String.format(
    				"dstIndex: %d, length: %d (expected: range(0, %d))", 
    				dstIndex, length, dst.length));
    	}
    	
    	if (length != 0) {
    		PlatformDependent.copyMemory(addr(index), dst, dstIndex, length);
    	}
    	return this;
    }
    
    @Override
    public ByteBuf copy(int index, int length) {
    	checkIndex(index, length);
    	ByteBuf copy = alloc().directBuffer(length, maxCapacity());
    	if (length != 0) {
    		if (copy.hasMemoryAddress()) {
    			PlatformDependent.copyMemory(addr(index), copy.memoryAddress(), length);
    			copy.setIndex(0, length);
    		} else {
    			copy.writeBytes(this, index, length);
    		}
    	}
    	return copy;
    }
    
    @Override
    public boolean hasMemoryAddress() {
    	return true;
    }
    
    @Override
    public long memoryAddress() {
    	return memoryAddress;
    }
    
    private long addr(int index) {
    	return memoryAddress + index;
    }
}
