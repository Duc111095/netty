package netty.common.util.internal;

import netty.common.util.IllegalReferenceCountException;
import netty.common.util.ReferenceCounted;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public abstract class ReferenceCountUpdater<T extends ReferenceCounted> {

	protected ReferenceCountUpdater() {
		
	}
	
	protected abstract void safeInitializeRawRefCnt(T refCntObj, int value);
	
	protected abstract int getAndAddRawRefCnt(T refCntObj, int increment);
	
	protected abstract int getRawRefCnt(T refCnt);
	
	protected abstract int getAcquireRawRefCnt(T refCnt);
	
	protected abstract void setReleaseRawRefCnt(T refCnt, int value);
	
	protected abstract boolean casRawRefCnt(T refCnt, int expected, int value);
	
	public final int initialValue() {
		return 2;
	}
	
	public final void setInitialValue(T instance) {
		safeInitializeRawRefCnt(instance, initialValue());
	}
	
	private static int realRefCnt(int rawCnt) {
		return rawCnt >>> 1;
	}
	
	public final int refCnt(T instance) {
		return realRefCnt(getAcquireRawRefCnt(instance));
	}
	
	public final boolean isLiveNonVolatile(T instance) {
		final int rawCnt = getRawRefCnt(instance);
		if(rawCnt == 2) {
			return true;
		}
		return (rawCnt & 1) == 0;
	}
	
	public final void setRefCnt(T instance, int refCnt) {
		int rawRefCnt = refCnt > 0 ? refCnt << 1 : 1;
		setReleaseRawRefCnt(instance, rawRefCnt);
	}
	
	public final void resetRefCnt(T instance) {
		setReleaseRawRefCnt(instance, initialValue());
	}
	
	public final T retain(T instance) {
		return retain0(instance, 2);
	}
	
	public final T retain0(T instance, int increment) {
		int oldRef = getAndAddRawRefCnt(instance, increment);
		if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
			getAndAddRawRefCnt(instance, -increment);
			throw new IllegalReferenceCountException(0, increment >>> 1);
		}
		return instance;
	}
	
	public final boolean release(T instance) {
		return release0(instance, 2);
	}
	
	public final boolean release(T instance, int decrement) {
		return release0(instance, checkPositive(decrement, "decrement") << 1);
	}
	
	private boolean release0(T instance, final int decrement) {
		int curr, next;
		do {
			curr = getRawRefCnt(instance);
			if (curr == decrement) {
				next = 1;
			} else {
				if (curr < decrement || (curr & 1) == 1) {
					throwIllegalRefCountOnRelease(decrement, curr);
				}
				next = curr - decrement;
			}
		} while (!casRawRefCnt(instance, curr, next));
		return (next & 1) == 1;
	}
	
	private static void throwIllegalRefCountOnRelease(int decrement, int curr) {
		throw new IllegalReferenceCountException(curr >>> 1, -(decrement >>> 1));
	}
	
	public enum UpdaterType {
		Unsafe,
		VarHandle,
		Atomic
	}
	
	public static <T extends ReferenceCounted> UpdaterType updaterTypeOf(Class<T> clz, String fieldName) {
		long fieldOffset = getUnsafeOffset(clz, fieldName);
		if (fieldOffset >= 0) {
			return UpdaterType.Unsafe;
		}
		if (PlatformDependent.hasVarHandle()) {
			return UpdaterType.VarHandle;
		}
		return UpdaterType.Atomic;
	}
	
	public static long getUnsafeOffset(Class<? extends ReferenceCounted> clz, String fieldName) {
		try {
			if (PlatformDependent.hasUnsafe()) {
				return PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
			}
		} catch (Throwable ignore) {
			
		}
		return -1;
	}
}
