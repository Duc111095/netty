package netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import netty.common.util.internal.CleanableDirectBuffer;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.UnstableApi;

public class UnpooledUnsafeDirectByteBuf extends UnpooledDirectByteBuf {
	private static final boolean USE_VAR_HANDLE = PlatformDependent.useVarHandleForMultiByteAccess();
	
	long memoryAddress;
	
	public UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
		super(alloc, initialCapacity, maxCapacity);
	}
	
	UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity,
			boolean permitExpensiveClean) {
		super(alloc, initialCapacity, maxCapacity, permitExpensiveClean);
	}
	
	protected UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, ByteBuffer initialBuffer, int maxCapacity) {
		super(alloc, initialBuffer, maxCapacity, false, true);
	}
	
	@UnstableApi
	protected UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, boolean slice,
			ByteBuffer initialBuffer, int maxCapacity) {
		super(alloc, initialBuffer, maxCapacity, false, slice);
	}
	
	UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, ByteBuffer initialBuffer, int maxCapacity, boolean doFree) {
		super(alloc, initialBuffer, maxCapacity, doFree, false);
	}
	
	@Override
	final void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
		super.setByteBuffer(buffer, tryFree);
		memoryAddress = PlatformDependent.directBufferAddress(buffer);
	}
	
	@Override
	final void setByteBuffer(CleanableDirectBuffer cleanableDirectBuffer, boolean tryFree) {
		super.setByteBuffer(cleanableDirectBuffer, tryFree);
		memoryAddress = PlatformDependent.directBufferAddress(cleanableDirectBuffer.buffer());
	}
	
	@Override
	public boolean hasMemoryAddress() {
		return true;
	}
	
	@Override
	public long memoryAddress() {
		ensureAccessible();
		return memoryAddress;
	}
	
	@Override
	public byte getByte(int index) {
		checkIndex(index);
		return _getByte(index);
	}
	
	@Override
	protected byte _getByte(int index) {
		return UnsafeByteBufUtil.getByte(addr(index));
	}
	
	@Override
	public short getShort(int index) {
		checkIndex(index, 2);
		return _getShort(index);
	}
	
	@Override
	protected short _getShort(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getShortBE(buffer, index);
		}
		return UnsafeByteBufUtil.getShort(addr(index));
	}
	
	@Override
	protected short _getShortLE(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getShortLE(buffer, index);
		}
		return UnsafeByteBufUtil.getShortLE(addr(index));
	}
	
	@Override
	public int getUnsignedMedium(int index) {
		checkIndex(index, 3);
		return _getUnsignedMedium(index);
	}
	
	@Override
	protected int _getUnsignedMedium(int index) {
		return UnsafeByteBufUtil.getUnsignedMedium(addr(index));
	}
	
	@Override
	protected int _getUnsignedMediumLE(int index) {
		return UnsafeByteBufUtil.getUnsignedMediumLE(addr(index));
	}
	
	@Override
	public int getInt(int index) {
		checkIndex(index, 4);
		return _getInt(index);
	}
	
	@Override
	protected int _getInt(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getIntBE(buffer, index);
		}
		return UnsafeByteBufUtil.getInt(addr(index));
	}
	
	@Override
	protected int _getIntLE(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getIntLE(buffer, index);
		}
		return UnsafeByteBufUtil.getIntLE(addr(index));
	}
	
	@Override
	public long getLong(int index) {
		checkIndex(index, 8);
		return _getLong(index);
	}
	
	@Override
	protected long _getLong(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getLongBE(buffer, index);
		}
		return UnsafeByteBufUtil.getLong(addr(index));
	}
	
	@Override
	protected long _getLongLE(int index) {
		if (USE_VAR_HANDLE) {
			return VarHandleByteBufferAccess.getLongLE(buffer, index);
		}
		return UnsafeByteBufUtil.getLongLE(addr(index));
	}
	
	@Override
	public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
		UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
		return this;
	}
	
	@Override
	void getBytes(int index, byte[] dst, int dstIndex, int length, boolean internal) {
		UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
	}
	
	@Override
	void getBytes(int index, ByteBuffer dst, boolean internal) {
		UnsafeByteBufUtil.getBytes(this, addr(index), index, dst);
	}
	
	@Override
	public ByteBuf setByte(int index, int value) {
		checkIndex(index);
		_setByte(index, value);
		return this;
	}
	
	@Override
	protected void _setByte(int index, int value) {
		UnsafeByteBufUtil.setByte(addr(index), value);
	}
	
	@Override
	public ByteBuf setShort(int index, int value) {
		checkIndex(index, 2);
		_setShort(index, value);
		return this;
	}
	
	@Override
	protected void _setShort(int index, int value) {
		if (USE_VAR_HANDLE) {
			VarHandleByteBufferAccess.setShortBE(buffer, index, value);
			return;
		}
		UnsafeByteBufUtil.setShort(addr(index), value);
	}
	
	@Override
	protected void _setShortLE(int index, int value) {
		if (USE_VAR_HANDLE) {
			VarHandleByteBufferAccess.setShortLE(buffer, index, value);
			return;
		}
		UnsafeByteBufUtil.setShortLE(addr(index), value);
	}
	
	@Override
    public ByteBuf setMedium(int index, int value) {
        checkIndex(index, 3);
        _setMedium(index, value);
        return this;
    }

    @Override
    protected void _setMedium(int index, int value) {
        UnsafeByteBufUtil.setMedium(addr(index), value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        UnsafeByteBufUtil.setMediumLE(addr(index), value);
    }

    @Override
    public ByteBuf setInt(int index, int value) {
        checkIndex(index, 4);
        _setInt(index, value);
        return this;
    }

    @Override
    protected void _setInt(int index, int value) {
        if (USE_VAR_HANDLE) {
            VarHandleByteBufferAccess.setIntBE(buffer, index, value);
            return;
        }
        UnsafeByteBufUtil.setInt(addr(index), value);
    }

    @Override
    protected void _setIntLE(int index, int value) {
        if (USE_VAR_HANDLE) {
            VarHandleByteBufferAccess.setIntLE(buffer, index, value);
            return;
        }
        UnsafeByteBufUtil.setIntLE(addr(index), value);
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        checkIndex(index, 8);
        _setLong(index, value);
        return this;
    }

    @Override
    protected void _setLong(int index, long value) {
        if (USE_VAR_HANDLE) {
            VarHandleByteBufferAccess.setLongBE(buffer, index, value);
            return;
        }
        UnsafeByteBufUtil.setLong(addr(index), value);
    }

    @Override
    protected void _setLongLE(int index, long value) {
        if (USE_VAR_HANDLE) {
            VarHandleByteBufferAccess.setLongLE(buffer, index, value);
            return;
        }
        UnsafeByteBufUtil.setLongLE(addr(index), value);
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        UnsafeByteBufUtil.setBytes(this, addr(index), index, src);
        return this;
    }

    @Override
    void getBytes(int index, OutputStream out, int length, boolean internal) throws IOException {
        UnsafeByteBufUtil.getBytes(this, addr(index), index, out, length);
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        return UnsafeByteBufUtil.setBytes(this, addr(index), index, in, length);
    }

    @Override
    public ByteBuf copy(int index, int length) {
        return UnsafeByteBufUtil.copy(this, addr(index), index, length);
    }
	
    final long addr(int index) {
    	return memoryAddress + index;
    }
    
    @Override
    protected SwappedByteBuf newSwappedByteBuf() {
    	if (PlatformDependent.isUnaligned()) {
    		return new UnsafeDirectSwappedByteBuf(this);
    	}
    	return super.newSwappedByteBuf();
    }
    
    @Override
    public ByteBuf setZero(int index, int length) {
    	checkIndex(index, length);
    	UnsafeByteBufUtil.setZero(addr(index), length);
    	return this;
    }
    
    @Override
    public ByteBuf writeZero(int length) {
    	ensureWritable(length);
    	int wIndex = writerIndex;
    	UnsafeByteBufUtil.setZero(addr(wIndex), length);
    	writerIndex = wIndex + length;
    	return this;
    }
} 
