package netty.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import netty.common.util.IllegalReferenceCountException;
import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.Recycler.Handle;

abstract class AbstractPooledDerivedByteBuf extends AbstractReferenceCountedByteBuf {
	private final EnhancedHandle<AbstractPooledDerivedByteBuf> recyclerHandle;
	private AbstractByteBuf rootParent;
	
	private ByteBuf parent;
	
	@SuppressWarnings("unchecked")
	AbstractPooledDerivedByteBuf(Handle<? extends AbstractPooledDerivedByteBuf> recyclerHandle) {
		super(0);
		this.recyclerHandle = (EnhancedHandle<AbstractPooledDerivedByteBuf>) recyclerHandle;
	}
	
	final void parent(ByteBuf newParent) {
		assert newParent instanceof SimpleLeakAwareByteBuf;
		parent = newParent;
	}
	
	@Override
	public final AbstractByteBuf unwrap() {
		AbstractByteBuf rootParent = this.rootParent;
		if (rootParent == null) {
			throw new IllegalReferenceCountException();
		}
		return rootParent;
	}
	
	final <U extends AbstractPooledDerivedByteBuf> U init(
			AbstractByteBuf unwrapped, ByteBuf wrapped, int readerIndex, int writerIndex, int maxCapacity) {
		wrapped.retain();
		parent = wrapped;
		rootParent = unwrapped;
		
		try {
			maxCapacity(maxCapacity);
			setIndex0(readerIndex, writerIndex);
			resetRefCnt();
			
			@SuppressWarnings("unchecked")
			final U castThis = (U) this;
			wrapped = null;
			return castThis;
		} finally {
			if (wrapped != null) {
				parent = rootParent = null;
				wrapped.release();
			}
		}
	}
	
	@Override
	protected final void deallocate() {
		ByteBuf parent = this.parent;
		this.parent = this.rootParent = null;
		recyclerHandle.unguardedRecycle(this);
		parent.release();
	}
	
	@Override
	public final ByteBufAllocator alloc() {
		return unwrap().alloc();
	}
	
	@Override
    public final ByteOrder order() {
        return unwrap().order();
    }

    @Override
    public boolean isReadOnly() {
        return unwrap().isReadOnly();
    }

    @Override
    public final boolean isDirect() {
        return unwrap().isDirect();
    }

    @Override
    public boolean hasArray() {
        return unwrap().hasArray();
    }

    @Override
    public byte[] array() {
        return unwrap().array();
    }

    @Override
    public boolean hasMemoryAddress() {
        return unwrap().hasMemoryAddress();
    }

    @Override
    public boolean isContiguous() {
        return unwrap().isContiguous();
    }

    @Override
    public final int nioBufferCount() {
        return unwrap().nioBufferCount();
    }

    @Override
    public final ByteBuffer internalNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }
    
    @Override
    public ByteBuf slice(int index, int length) {
    	ensureAccessible();
    	return new PooledNonRetainedSlicedByteBuf(this, unwrap(), index, length);
    }
    
    final ByteBuf duplicate0() {
    	ensureAccessible();
    	return new PooledNonRetainedDuplicateByteBuf(this, unwrap());
    }
    
    private static final class PooledNonRetainedDuplicateByteBuf extends UnpooledDuplicatedByteBuf {
        private final ByteBuf referenceCountDelegate;

        PooledNonRetainedDuplicateByteBuf(ByteBuf referenceCountDelegate, AbstractByteBuf buffer) {
            super(buffer);
            this.referenceCountDelegate = referenceCountDelegate;
        }

        @Override
        boolean isAccessible0() {
            return referenceCountDelegate.isAccessible();
        }

        @Override
        int refCnt0() {
            return referenceCountDelegate.refCnt();
        }

        @Override
        ByteBuf retain0() {
            referenceCountDelegate.retain();
            return this;
        }

        @Override
        ByteBuf retain0(int increment) {
            referenceCountDelegate.retain(increment);
            return this;
        }

        @Override
        ByteBuf touch0() {
            referenceCountDelegate.touch();
            return this;
        }

        @Override
        ByteBuf touch0(Object hint) {
            referenceCountDelegate.touch(hint);
            return this;
        }

        @Override
        boolean release0() {
            return referenceCountDelegate.release();
        }

        @Override
        boolean release0(int decrement) {
            return referenceCountDelegate.release(decrement);
        }

        @Override
        public ByteBuf duplicate() {
            ensureAccessible();
            return new PooledNonRetainedDuplicateByteBuf(referenceCountDelegate, this);
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return PooledDuplicatedByteBuf.newInstance(unwrap(), this, readerIndex(), writerIndex());
        }

        @Override
        public ByteBuf slice(int index, int length) {
            checkIndex(index, length);
            return new PooledNonRetainedSlicedByteBuf(referenceCountDelegate, unwrap(), index, length);
        }

        @Override
        public ByteBuf retainedSlice() {
            // Capacity is not allowed to change for a sliced ByteBuf, so length == capacity()
            return retainedSlice(readerIndex(), capacity());
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return PooledSlicedByteBuf.newInstance(unwrap(), this, index, length);
        }
    }

    private static final class PooledNonRetainedSlicedByteBuf extends UnpooledSlicedByteBuf {
        private final ByteBuf referenceCountDelegate;

        PooledNonRetainedSlicedByteBuf(ByteBuf referenceCountDelegate,
                                       AbstractByteBuf buffer, int index, int length) {
            super(buffer, index, length);
            this.referenceCountDelegate = referenceCountDelegate;
        }

        @Override
        boolean isAccessible0() {
            return referenceCountDelegate.isAccessible();
        }

        @Override
        int refCnt0() {
            return referenceCountDelegate.refCnt();
        }

        @Override
        ByteBuf retain0() {
            referenceCountDelegate.retain();
            return this;
        }

        @Override
        ByteBuf retain0(int increment) {
            referenceCountDelegate.retain(increment);
            return this;
        }

        @Override
        ByteBuf touch0() {
            referenceCountDelegate.touch();
            return this;
        }

        @Override
        ByteBuf touch0(Object hint) {
            referenceCountDelegate.touch(hint);
            return this;
        }

        @Override
        boolean release0() {
            return referenceCountDelegate.release();
        }

        @Override
        boolean release0(int decrement) {
            return referenceCountDelegate.release(decrement);
        }

        @Override
        public ByteBuf duplicate() {
            ensureAccessible();
            return new PooledNonRetainedDuplicateByteBuf(referenceCountDelegate, unwrap())
                    .setIndex(idx(readerIndex()), idx(writerIndex()));
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return PooledDuplicatedByteBuf.newInstance(unwrap(), this, idx(readerIndex()), idx(writerIndex()));
        }

        @Override
        public ByteBuf slice(int index, int length) {
            checkIndex(index, length);
            return new PooledNonRetainedSlicedByteBuf(referenceCountDelegate, unwrap(), idx(index), length);
        }

        @Override
        public ByteBuf retainedSlice() {
            // Capacity is not allowed to change for a sliced ByteBuf, so length == capacity()
            return retainedSlice(0, capacity());
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return PooledSlicedByteBuf.newInstance(unwrap(), this, idx(index), length);
        }
    }
}
