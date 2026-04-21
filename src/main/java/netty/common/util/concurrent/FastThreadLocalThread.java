package netty.common.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;

import netty.common.util.internal.InternalThreadLocalMap;
import netty.common.util.internal.LongLongHashMap;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public class FastThreadLocalThread extends Thread {
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(FastThreadLocalThread.class);
	private static final AtomicReference<FallbackThreadSet> fallbackThreads =
			new AtomicReference<>(FallbackThreadSet.EMPTY);
	private final boolean cleanupFastThreadLocals;
	private InternalThreadLocalMap threadLocalMap;
	
	public FastThreadLocalThread() {
		cleanupFastThreadLocals = false;
	}
	
	public FastThreadLocalThread(Runnable target) {
		super(FastThreadLocalRunnable.wrap(target));
		cleanupFastThreadLocals = true;
	}
	
	public FastThreadLocalThread(ThreadGroup group, Runnable target) {
		super(group, FastThreadLocalRunnable.wrap(target));
		cleanupFastThreadLocals = true;
	}
	
	public FastThreadLocalThread(String name) {
		super(name);
		cleanupFastThreadLocals = false;
	}
	
	public FastThreadLocalThread(ThreadGroup group, String name) {
		super(group, name);
		cleanupFastThreadLocals = false;
	}
	
	public FastThreadLocalThread(Runnable target, String name) {
		super(FastThreadLocalRunnable.wrap(target), name);
		cleanupFastThreadLocals = true;
	}
	
	public FastThreadLocalThread(ThreadGroup group, Runnable target, String name) {
		super(group, FastThreadLocalRunnable.wrap(target), name);
		cleanupFastThreadLocals = true;
	}
	
	public FastThreadLocalThread(ThreadGroup group, Runnable target, String name, long stackSize) {
		super(group, FastThreadLocalRunnable.wrap(target), name, stackSize);
		cleanupFastThreadLocals = true;
	}
	
	public final InternalThreadLocalMap threadLocalMap() {
		if (this != Thread.currentThread() && logger.isWarnEnabled()) {
			logger.warn(new RuntimeException("It's not thread-safe to get 'threadLocalMap' " + 
					"which doesn't belong to the caller thread"));
		}
		return threadLocalMap;
	}
	
	public final void setThreadLocalMap(InternalThreadLocalMap threadLocalMap) {
		if (this != Thread.currentThread() && logger.isWarnEnabled()) {
			logger.warn(new RuntimeException("It's not thread-safe to set 'threadLocalMap' " +
					"which doesn't belong to the caller thread"));
		}
		this.threadLocalMap = threadLocalMap;
	}
	
	public boolean willCleanupFastThreadLocals() {
		return cleanupFastThreadLocals;
	}
	
	public static boolean willCleanupFastThreadLocals(Thread thread) {
		return thread instanceof FastThreadLocalThread  &&
				((FastThreadLocalThread) thread).willCleanupFastThreadLocals();
	}
	
	public static boolean currentThreadWillCleanupFastThreadLocals() {
		Thread currentThread = currentThread();
		if (currentThread instanceof FastThreadLocalThread) {
			return ((FastThreadLocalThread) currentThread).willCleanupFastThreadLocals();
		}
		return isFastThreadLocalVirtualThread();
	}
	
	public static boolean currentThreadHasFastThreadLocal() {
		return currentThread() instanceof FastThreadLocalThread || isFastThreadLocalVirtualThread();
	}
	
	private static boolean isFastThreadLocalVirtualThread() {
		return fallbackThreads.get().contains(currentThread().getId());
	}
	
	public static void runWithFastThreadLocal(Runnable runnable) {
		Thread current = currentThread();
		if (current instanceof FastThreadLocalThread) {
			throw new IllegalStateException("Caller is a real FastThreadLocalThread");
		}
		long id = current.getId();
		fallbackThreads.updateAndGet(set -> {
			if (set.contains(id)) {
				throw new IllegalStateException("Reentrant call to run()");
			}
			return set.add(id);
		});
		
		try {
			runnable.run();
		} finally {
			fallbackThreads.getAndUpdate(set -> set.remove(id));
			FastThreadLocal.removeAll();
		}
	}
	
	public boolean permitBlockingCalls() {
		return false;
	}
	
	private static final class FallbackThreadSet {
		static final FallbackThreadSet EMPTY = new FallbackThreadSet();
		private static final long EMPTY_VALUE = 0L;
		
		private final LongLongHashMap map;
		
		private FallbackThreadSet() {
			this.map = new LongLongHashMap(EMPTY_VALUE);
		}
		
		private FallbackThreadSet(LongLongHashMap map) {
			this.map = map;
		}
		
		public boolean contains(long threadId) {
			long key = threadId >>> 6;
			long bit = 1L << (threadId & 63);
			
			long bitmap = map.get(key);
			return (bitmap & bit) != 0;
		}
		
		public FallbackThreadSet add(long threadId) {
			long key = threadId >>> 6;
			long bit = 1L << (threadId & 63);
			
			LongLongHashMap newMap = new LongLongHashMap(map);
			long oldBitmap = newMap.get(key);
			long newBitmap = oldBitmap | bit;
			newMap.put(key, newBitmap);
			
			return new FallbackThreadSet(newMap);
		}
		
		public FallbackThreadSet remove(long threadId) {
			long key = threadId >>> 6;
			long bit = 1L << (threadId & 63);
			long oldBitmap = map.get(key);
			if ((oldBitmap & bit) == 0) {
				return this;
			}
			
			LongLongHashMap newMap = new LongLongHashMap(map);
			long newBitmap = oldBitmap & ~bit;
			
			if (newBitmap != EMPTY_VALUE) {
				newMap.put(key, newBitmap);
			} else {
				newMap.remove(key);
			}
			
			return new FallbackThreadSet(newMap);
		}
		
		
	}
}
