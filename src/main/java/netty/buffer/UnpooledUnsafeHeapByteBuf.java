package netty.buffer;

import netty.common.util.internal.PlatformDependent;

public class UnpooledUnsafeHeapByteBuf extends UnpooledHeapByteBuf {

	public UnpooledUnsafeHeapByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
		super(alloc, initialCapacity, maxCapacity);
	}
	
	@Override
	protected byte[] allocateArray(int initialCapacity) {
		return PlatformDependent.allocateUninitializedArray(initialCapacity);
	}
	
	@Override
	public byte getByte(int index) {
		checkIndex(index);
		return _getByte(index);
	}
	
	@Override
    protected byte _getByte(int index) {
        return UnsafeByteBufUtil.getByte(array, index);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, 2);
        return _getShort(index);
    }

    @Override
    protected short _getShort(int index) {
        return UnsafeByteBufUtil.getShort(array, index);
    }

    @Override
    public short getShortLE(int index) {
        checkIndex(index, 2);
        return _getShortLE(index);
    }

    @Override
    protected short _getShortLE(int index) {
        return UnsafeByteBufUtil.getShortLE(array, index);
    }

    @Override
    public int getUnsignedMedium(int index) {
        checkIndex(index, 3);
        return _getUnsignedMedium(index);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return UnsafeByteBufUtil.getUnsignedMedium(array, index);
    }

    @Override
    public int getUnsignedMediumLE(int index) {
        checkIndex(index, 3);
        return _getUnsignedMediumLE(index);
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        return UnsafeByteBufUtil.getUnsignedMediumLE(array, index);
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, 4);
        return _getInt(index);
    }

    @Override
    protected int _getInt(int index) {
        return UnsafeByteBufUtil.getInt(array, index);
    }

    @Override
    public int getIntLE(int index) {
        checkIndex(index, 4);
        return _getIntLE(index);
    }

    @Override
    protected int _getIntLE(int index) {
        return UnsafeByteBufUtil.getIntLE(array, index);
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, 8);
        return _getLong(index);
    }

    @Override
    protected long _getLong(int index) {
        return UnsafeByteBufUtil.getLong(array, index);
    }

    @Override
    public long getLongLE(int index) {
        checkIndex(index, 8);
        return _getLongLE(index);
    }

    @Override
    protected long _getLongLE(int index) {
        return UnsafeByteBufUtil.getLongLE(array, index);
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        checkIndex(index);
        _setByte(index, value);
        return this;
    }

    @Override
    protected void _setByte(int index, int value) {
        UnsafeByteBufUtil.setByte(array, index, value);
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        checkIndex(index, 2);
        _setShort(index, value);
        return this;
    }

    @Override
    protected void _setShort(int index, int value) {
        UnsafeByteBufUtil.setShort(array, index, value);
    }

    @Override
    public ByteBuf setShortLE(int index, int value) {
        checkIndex(index, 2);
        _setShortLE(index, value);
        return this;
    }

    @Override
    protected void _setShortLE(int index, int value) {
        UnsafeByteBufUtil.setShortLE(array, index, value);
    }

    @Override
    public ByteBuf setMedium(int index, int   value) {
        checkIndex(index, 3);
        _setMedium(index, value);
        return this;
    }

    @Override
    protected void _setMedium(int index, int value) {
        UnsafeByteBufUtil.setMedium(array, index, value);
    }

    @Override
    public ByteBuf setMediumLE(int index, int   value) {
        checkIndex(index, 3);
        _setMediumLE(index, value);
        return this;
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        UnsafeByteBufUtil.setMediumLE(array, index, value);
    }

    @Override
    public ByteBuf setInt(int index, int   value) {
        checkIndex(index, 4);
        _setInt(index, value);
        return this;
    }

    @Override
    protected void _setInt(int index, int value) {
        UnsafeByteBufUtil.setInt(array, index, value);
    }

    @Override
    public ByteBuf setIntLE(int index, int   value) {
        checkIndex(index, 4);
        _setIntLE(index, value);
        return this;
    }

    @Override
    protected void _setIntLE(int index, int value) {
        UnsafeByteBufUtil.setIntLE(array, index, value);
    }

    @Override
    public ByteBuf setLong(int index, long  value) {
        checkIndex(index, 8);
        _setLong(index, value);
        return this;
    }

    @Override
    protected void _setLong(int index, long value) {
        UnsafeByteBufUtil.setLong(array, index, value);
    }

    @Override
    public ByteBuf setLongLE(int index, long  value) {
        checkIndex(index, 8);
        _setLongLE(index, value);
        return this;
    }

    @Override
    protected void _setLongLE(int index, long value) {
        UnsafeByteBufUtil.setLongLE(array, index, value);
    }

    @Override
    public ByteBuf setZero(int index, int length) {
        checkIndex(index, length);
        UnsafeByteBufUtil.setZero(array, index, length);
        return this;
    }

    @Override
    public ByteBuf writeZero(int length) {
        ensureWritable(length);
        int wIndex = writerIndex;
        UnsafeByteBufUtil.setZero(array, wIndex, length);
        writerIndex = wIndex + length;
        return this;
    }

    @Override
    protected SwappedByteBuf newSwappedByteBuf() {
        if (PlatformDependent.isUnaligned()) {
            // Only use if unaligned access is supported otherwise there is no gain.
            return new UnsafeHeapSwappedByteBuf(this);
        }
        return super.newSwappedByteBuf();
    }
}
