package netty.common.util.internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import netty.common.util.IllegalReferenceCountException;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public final class RefCnt {
	private static final int UNSAFE = 0;
	private static final int VAR_HANDLE = 1;
	private static final int ATOMIC_UPDATER = 2;
	private static final int REF_CNT_IMPL;
	
	static {
		if (PlatformDependent.hasUnsafe()) {
			REF_CNT_IMPL = UNSAFE;
		} else if (PlatformDependent.hasVarHandle()) {
			REF_CNT_IMPL = VAR_HANDLE;
		} else {
			REF_CNT_IMPL = ATOMIC_UPDATER;
		}
	}
	
	volatile int value;
	
	public RefCnt() {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			UnsafeRefCnt.init(this);
			break;
		case VAR_HANDLE:
			VarHandleRefCnt.init(this);
			break;
		case ATOMIC_UPDATER:
		default:
			AtomicRefCnt.init(this);
			break;
		}
	}
	
	public static int refCnt(RefCnt ref) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			return UnsafeRefCnt.refCnt(ref);
		case VAR_HANDLE:
			return VarHandleRefCnt.refCnt(ref);
		case ATOMIC_UPDATER:
		default:
			return AtomicRefCnt.refCnt(ref);
		}
	}
	
	public static void retain(RefCnt ref) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			UnsafeRefCnt.retain(ref);
			break;
		case VAR_HANDLE:
			VarHandleRefCnt.retain(ref);
		case ATOMIC_UPDATER:
		default:
			AtomicRefCnt.retain(ref);
			break;
		}
	}
	
	public static void retain(RefCnt ref, int increment) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			UnsafeRefCnt.retain(ref, increment);
			break;
		case VAR_HANDLE:
			VarHandleRefCnt.retain(ref, increment);
			break;
		case ATOMIC_UPDATER:
		default:
			AtomicRefCnt.retain(ref, increment);
			break;
		}
	}
	
	public static boolean release(RefCnt ref) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			return UnsafeRefCnt.release(ref);
		case VAR_HANDLE:
			return VarHandleRefCnt.release(ref);
		case ATOMIC_UPDATER:
		default:
			return AtomicRefCnt.release(ref);
		}
	}
	
	public static boolean release(RefCnt ref, int decrement) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			return UnsafeRefCnt.release(ref, decrement);
		case VAR_HANDLE:
			return VarHandleRefCnt.release(ref, decrement);
		case ATOMIC_UPDATER:
		default:
			return AtomicRefCnt.release(ref, decrement);
		}
	}
	
	public static boolean isLiveNonVolatile(RefCnt ref) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			return UnsafeRefCnt.isLiveNonVolatile(ref);
		case VAR_HANDLE:
			return VarHandleRefCnt.isLiveNonVolatile(ref);
		case ATOMIC_UPDATER:
		default:
			return AtomicRefCnt.isLiveNonVolatile(ref);
		}
	}
	
	public static void setRefCnt(RefCnt ref, int refCnt) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			UnsafeRefCnt.setRefCnt(ref, refCnt);
			break;
		case VAR_HANDLE:
			VarHandleRefCnt.setRefCnt(ref, refCnt);
			break;
		case ATOMIC_UPDATER:
		default:
			AtomicRefCnt.setRefCnt(ref, refCnt);
			break;
		}
	}
	
	public static void resetRefCnt(RefCnt ref) {
		switch (REF_CNT_IMPL) {
		case UNSAFE:
			UnsafeRefCnt.resetRefCnt(ref);
			break;
		case VAR_HANDLE:
			VarHandleRefCnt.resetRefCnt(ref);
			break;
		case ATOMIC_UPDATER:
		default:
			AtomicRefCnt.resetRefCnt(ref);
			break;
		}
	}
	
	static void throwIllegalRefCountOnRelease(int decrement, int curr) {
		throw new IllegalReferenceCountException(curr >>> 1, -(decrement >>> 1));
	}
	
	private static final class AtomicRefCnt {
		private static final AtomicIntegerFieldUpdater<RefCnt> UPDATER =
				AtomicIntegerFieldUpdater.newUpdater(RefCnt.class, "value");
		static void init(RefCnt instance) {
			UPDATER.set(instance, 2);
		}
		
		static int refCnt(RefCnt instance) {
			return UPDATER.get(instance) >>> 1;
		}
		
		static void retain(RefCnt instance) {
			retain0(instance, 2);
		}
		
		static void retain(RefCnt instance, int increment) {
			retain0(instance, checkPositive(increment, "increment") << 1);
		}
		
		private static void retain0(RefCnt instance, int increment) {
			int oldRef = UPDATER.getAndAdd(instance, increment);
			if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
				UPDATER.getAndAdd(instance, -increment);
				throw new IllegalReferenceCountException(0, increment >>> 1);
			}
		}
		
		static boolean release(RefCnt instance) {
			return release0(instance, 2);
		}
		
		static boolean release(RefCnt instance, int decrement) {
			return release0(instance, checkPositive(decrement, "decrement") << 1);
		}
		
		private static boolean release0(RefCnt instance, int decrement) {
			int curr, next;
			do {
				curr = instance.value;
				if (curr == decrement) {
					next = 1;
				} else {
					if (curr < decrement || (curr & 1) == 1) {
						throwIllegalRefCountOnRelease(decrement, curr);
					}
					next = curr - decrement;
				}
			} while (!UPDATER.compareAndSet(instance, curr, next));
			return (next & 1) == 1;
		}
		
		static void setRefCnt(RefCnt instance, int refCnt) {
			int rawRefCnt = refCnt > 0 ? refCnt << 1 : 1;
			UPDATER.lazySet(instance, rawRefCnt);
		}
		
		static void resetRefCnt(RefCnt instance) {
			UPDATER.lazySet(instance, 2);
		}
		
		static boolean isLiveNonVolatile(RefCnt instance) {
			final int rawCnt = instance.value;
			if (rawCnt == 2) {
				return true;
			}
			return (rawCnt & 1) == 0;
		}
	}
	
	private static final class VarHandleRefCnt {
		private static final VarHandle VH;
		
		static {
			VH = PlatformDependent.findVarHandleOfIntField(MethodHandles.lookup(), RefCnt.class, "value");
		}
		
		static void init(RefCnt instance) {
			VH.set(instance, 2);
			VarHandle.storeStoreFence();
		}
		
		static int refCnt(RefCnt instance) {
			return (int) VH.getAcquire(instance) >>> 1;
		}
		
		static void retain(RefCnt instance) {
			retain0(instance, 2);
		}
		
		static void retain(RefCnt instance, int increment) {
			retain0(instance, checkPositive(increment, "increment") << 1);
		}
		
		private static void retain0(RefCnt instance, int increment) {
			int oldRef = (int) VH.getAndAdd(instance, increment);
			if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
				VH.getAndAdd(instance, -increment);
				throw new IllegalReferenceCountException(0, increment >>> 1);
			}
		}
		
		static boolean release(RefCnt instance) {
			return release0(instance, 2);
		}
		
		static boolean release(RefCnt instance, int decrement) {
			return release0(instance, checkPositive(decrement, "decrement") << 1);
		}
		
		private static boolean release0(RefCnt instance, int decrement) {
			int curr, next;
			do {
				curr = (int) VH.get(instance);
				if (curr == decrement) {
					next = 1;
				} else {
					if (curr < decrement || (curr & 1) == 1) {
						throwIllegalRefCountOnRelease(decrement, curr);
					}
					next = curr - decrement;
				}
			} while (!(boolean) VH.compareAndSet(instance, curr, next));
			return (next & 1) == 1;
		}
		
		static void setRefCnt(RefCnt instance, int refCnt) {
			int rawRefCnt = refCnt > 0 ? refCnt << 1 : 1;
			VH.setRelease(instance, rawRefCnt);
		}
		
		static void resetRefCnt(RefCnt instance) {
			VH.setRelease(instance, 2);
		}
		
		static boolean isLiveNonVolatile(RefCnt instance) {
			final int rawCnt = (int) VH.get(instance);
			if (rawCnt == 2) {
				return true;
			}
			return (rawCnt & 1) == 0;
		}
	}
	
	private static final class UnsafeRefCnt {
		private static final long VALUE_OFFSET = getUnsafeOffset(RefCnt.class, "value");
		
		private static long getUnsafeOffset(Class<?> clz, String fieldName) {
			try {
				if (PlatformDependent.hasUnsafe()) {
					return PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
				}
			} catch (Throwable ignore) {
				
			}
			return -1;
		}
		
		static void init(RefCnt instance) {
			PlatformDependent.safeConstructPutInt(instance, VALUE_OFFSET, 2);
		}
		
		static int refCnt(RefCnt instance) {
			return PlatformDependent.getVolatileInt(instance, VALUE_OFFSET) >>> 1;
		}
		
		static void retain(RefCnt instance) {
			retain0(instance, 2);
		}
		
		static void retain(RefCnt instance, int increment) {
			retain0(instance, checkPositive(increment, "increment") << 1);
		}
		
		private static void retain0(RefCnt instance, int increment) {
			int oldRef = PlatformDependent.getAndAddInt(instance, VALUE_OFFSET, increment);
			if ((oldRef & 0x80000001) != 0 || oldRef > Integer.MAX_VALUE - increment) {
				PlatformDependent.getAndAddInt(instance, VALUE_OFFSET, -increment);
				throw new IllegalReferenceCountException(0, increment >>> 1);
			}
		}
		
		static boolean release(RefCnt instance) {
			return release0(instance, 2);
		}
		
		static boolean release(RefCnt instance, int decrement) {
			return release0(instance, checkPositive(decrement, "decrement") << 1);
		}
		
		private static boolean release0(RefCnt instance, int decrement) {
			int curr, next;
			do {
				curr = PlatformDependent.getInt(instance, VALUE_OFFSET);
				if (curr == decrement) {
					next = 1;
				} else {
					if (curr < decrement || (curr & 1) == 1) {
						throwIllegalRefCountOnRelease(decrement, curr);
					}
					next = curr - decrement;
				}
			} while (!PlatformDependent.compareAndSwapInt(instance, VALUE_OFFSET, curr, next));
			return (next & 1) == 1;
		}
		
		static void setRefCnt(RefCnt instance, int refCnt) {
			int rawRefCnt = refCnt > 0 ? refCnt << 1 : 1;
			PlatformDependent.putOrderedInt(instance, VALUE_OFFSET, refCnt);
		}
		
		static void resetRefCnt(RefCnt instance) {
			PlatformDependent.putOrderedInt(instance, VALUE_OFFSET, 2);
		}
		
		static boolean isLiveNonVolatile(RefCnt instance) {
			final int rawCnt = PlatformDependent.getInt(instance, VALUE_OFFSET);
			if (rawCnt == 2) {
				return true;
			}
			return (rawCnt & 1) == 0;
		}
	}
}
