package netty.buffer;

import java.nio.ByteOrder;

import netty.common.util.IllegalReferenceCountException;
import netty.common.util.ResourceLeakTracker;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.ThrowableUtil;

class SimpleLeakAwareByteBuf extends WrappedByteBuf {
	
	private final ByteBuf trackedByteBuf;
	final ResourceLeakTracker<ByteBuf> leak;
	
	SimpleLeakAwareByteBuf(ByteBuf wrapped, ByteBuf trackedByteBuf, ResourceLeakTracker<ByteBuf> leak) {
		super(wrapped);
		this.trackedByteBuf = ObjectUtil.checkNotNull(trackedByteBuf, "trackedByteBuf");
		this.leak = ObjectUtil.checkNotNull(leak, "leak");
	}
	
	SimpleLeakAwareByteBuf(ByteBuf wrapped, ResourceLeakTracker<ByteBuf> leak) {
		this(wrapped, wrapped, leak);
	}
	
	@Override
	public ByteBuf slice() {
		return newSharedLeakAwareByteBuf(super.slice());
	}
	
	@Override
	public ByteBuf retainedSlice() {
		try {
			return unwrappedDerived(super.retainedSlice());
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public ByteBuf retainedSlice(int index, int length) {
		try {
			return unwrappedDerived(super.retainedSlice(index, length));
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public ByteBuf retainedDuplicate() {
		try {
			return unwrappedDerived(super.retainedDuplicate());
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public ByteBuf readRetainedSlice(int length) {
		try {
			return unwrappedDerived(super.readRetainedSlice(length));
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public ByteBuf slice(int index, int length) {
		return newSharedLeakAwareByteBuf(super.slice(index, length));
	}
	
	@Override
	public ByteBuf duplicate() {
		return newSharedLeakAwareByteBuf(super.duplicate());
	}
	
	@Override
	public ByteBuf readSlice(int length) {
		return newSharedLeakAwareByteBuf(super.readSlice(length));
	}
	
	@Override
	public ByteBuf asReadOnly() {
		return newSharedLeakAwareByteBuf(super.asReadOnly());
	}
	
	@Override
	public ByteBuf touch() {
		return this;
	}
	
	@Override
	public ByteBuf retain() {
		try {
			return super.retain();
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public ByteBuf retain(int increment) {
		try {
			return super.retain(increment);
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public boolean release() {
		try {
			if (super.release()) {
				closeLeak();
				return true;
			}
			return false;
		} catch (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	@Override
	public boolean release(int decrement) {
		try {
			if (super.release(decrement)) {
				closeLeak();
				return true;
			}
			return false;
		} catch  (IllegalReferenceCountException irce) {
			ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
			throw irce;
		}
	}
	
	private void closeLeak() {
		boolean closed  = leak.close(trackedByteBuf);
		assert closed;
	}
	
	@Override
	public ByteBuf order(ByteOrder endianess) {
		if (order() == endianess) {
			return this;
		} else {
			return newSharedLeakAwareByteBuf(super.order(endianess));
		}
	}
	
	private ByteBuf unwrappedDerived(ByteBuf derived) {
		ByteBuf unwrappedDerived = unwrapSwapped(derived);
		if (unwrappedDerived instanceof AbstractPooledDerivedByteBuf) {
			((AbstractPooledDerivedByteBuf) unwrappedDerived).parent(this);
			return newLeakAwareByteBuf(derived, AbstractByteBuf.leakDetector.trackForcibly(derived));
		}
		return newSharedLeakAwareByteBuf(derived);
	}
	
	private static ByteBuf unwrapSwapped(ByteBuf buf) {
		if (buf instanceof SwappedByteBuf) {
			do {
				buf = buf.unwrap();
			} while (buf instanceof SwappedByteBuf);
			return buf;
		}
		return buf;
	}
	
	private SimpleLeakAwareByteBuf newSharedLeakAwareByteBuf(
			ByteBuf wrapped) {
		return newLeakAwareByteBuf(wrapped, trackedByteBuf, leak);
	}
	
	private SimpleLeakAwareByteBuf newLeakAwareByteBuf(
			ByteBuf wrapped, ResourceLeakTracker<ByteBuf> leakTracker) {
		return newLeakAwareByteBuf(wrapped, wrapped, leakTracker);
	}
	
	protected SimpleLeakAwareByteBuf newLeakAwareByteBuf(
			ByteBuf buf, ByteBuf trackedByteBuf, ResourceLeakTracker<ByteBuf> leakTracker) {
		return new SimpleLeakAwareByteBuf(buf, trackedByteBuf, leakTracker);
	}
}
