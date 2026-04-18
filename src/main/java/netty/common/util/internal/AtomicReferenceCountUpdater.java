package netty.common.util.internal;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import netty.common.util.ReferenceCounted;

public abstract class AtomicReferenceCountUpdater<T extends ReferenceCounted> extends ReferenceCountUpdater<T> {

	protected AtomicReferenceCountUpdater() {
		
	}
	
	protected abstract AtomicIntegerFieldUpdater<T> updater();
	
	@Override
	protected final void safeInitializeRawRefCnt(T refCntObj, int value) {
		updater().set(refCntObj, value);
	}
	
	@Override
	protected final int getAndAddRawRefCnt(T refCntObj, int increment) {
		return updater().getAndAdd(refCntObj, increment);
	}
	
	@Override
	protected final int getRawRefCnt(T refCnt) {
		return updater().get(refCnt);
	}
	
	@Override
	protected final int getAcquireRawRefCnt(T refCnt) {
		return updater().get(refCnt);
	}
	
	@Override
	protected final void setReleaseRawRefCnt(T refCnt, int value) {
		updater().lazySet(refCnt, value);
	}
	
	@Override
	protected final boolean casRawRefCnt(T refCnt, int expected, int value) {
		return updater().compareAndSet(refCnt, expected, value);
	}
} 
