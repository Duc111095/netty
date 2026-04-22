package netty.common.util;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.VisibleForTesting;

import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.concurrent.FastThreadLocalThread;
import netty.common.util.internal.ObjectPool;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.UnstableApi;

import static netty.common.util.internal.PlatformDependent.newFixedMpmcQueue;
import static netty.common.util.internal.PlatformDependent.newMpscQueue;


public abstract class Recycler<T> {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(Recycler.class);
	
	private static final class LocalPoolHandle<T> extends EnhancedHandle<T> {
		private final UnguardedLocalPool<T> pool;
		
		private LocalPoolHandle(UnguardedLocalPool<T> pool) {
			this.pool = pool;
		}
		
		@Override
		public void recycle(T object) {
			UnguardedLocalPool<T> pool = this.pool;
			if (pool != null) {
				pool.release(object);
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void unguardedRecycle(final Object object) {
			UnguardedLocalPool<T> pool = this.pool;
			if (pool != null) {
				pool.release((T) object);
			}
		}
	}
	
	private static final EnhancedHandle<?> NOOP_HANDLE = new LocalPoolHandle<>(null);
	private static final UnguardedLocalPool<?> NOOP_LOCAL_POOL = new UnguardedLocalPool<>(0);
	private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024;
	private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
	private static final int RATIO;
	private static final int DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD;
	private static final boolean BLOCKING_POOL;
	private static final boolean BATCH_FAST_TL_ONLY;
	
	static {
		int maxCapacityPerThread = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacityPerThread", 
				SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD));
		if (maxCapacityPerThread < 0) {
			maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
		}
		
		DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
		DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD = SystemPropertyUtil.getInt("io.netty.recycler.chunkSize", 32);
		
		RATIO = Math.max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));
		
		BLOCKING_POOL = SystemPropertyUtil.getBoolean("io.netty.recycler.blocking", false);
		BATCH_FAST_TL_ONLY = SystemPropertyUtil.getBoolean("io.netty.recycler.batchFastThreadLocalOnly", true);
		
		if (logger.isDebugEnabled()) {
			if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
				logger.debug("-Dio.netty.recycler.maxCapacityPerThread: disabled");
				logger.debug("-Dio.netty.recycler.ratio: disabled");
				logger.debug("-Dio.netty.recycler.chunkSize: disabled");
				logger.debug("-Dio.netty.recycler.blocking: disabled");
				logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: disabled");
			} else {
				logger.debug("-Dio.netty.recycler.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
				logger.debug("-Dio.netty.recycler.ratio: {}", RATIO);
				logger.debug("-Dio.netty.recycler.chunkSize: {}", DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
				logger.debug("-Dio.netty.recycler.blocking: {}", BLOCKING_POOL);
				logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: {}", BATCH_FAST_TL_ONLY);
			}
		}
	}
	
	private final LocalPool<?, T> localPool;
	private final FastThreadLocal<LocalPool<?, T>> threadLocalPool;
	
	
	@SuppressWarnings("unchecked")
	protected Recycler(int maxCapacity, boolean unguarded) {
		if (maxCapacity <= 0) {
			maxCapacity = 0;
		} else {
			maxCapacity = Math.max(4, maxCapacity);
		}
		threadLocalPool = null;
		if (maxCapacity == 0) {
			localPool = (LocalPool<?, T>) NOOP_LOCAL_POOL;
		} else {
			localPool = unguarded ? new UnguardedLocalPool<>(maxCapacity) : new GuardedLocalPool<>(maxCapacity);
		}
	}
	
	protected Recycler(boolean unguarded) {
		this(DEFAULT_MAX_CAPACITY_PER_THREAD, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD, unguarded);
	}
	
	protected Recycler(Thread owner, boolean unguarded) {
		this(DEFAULT_MAX_CAPACITY_PER_THREAD, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD, owner, unguarded);
	}
	
	protected Recycler(int maxCapacityPerThread) {
		this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
	}
	
	protected Recycler() {
		this(DEFAULT_MAX_CAPACITY_PER_THREAD);
	}
	
	protected Recycler(int chunksSize, int maxCapacityPerThread, boolean unguarded) {
		this(maxCapacityPerThread, RATIO, chunksSize, unguarded);
	}
	
	protected Recycler(int chunkSize, int maxCapacityPerThread, Thread owner, boolean unguarded) {
		this(maxCapacityPerThread, RATIO, chunkSize, owner, unguarded);
	}
	
	protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor) {
		this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
	}
	
	protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
			int ratio, int maxDelayedQueuesPerThread) {
		this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
	}
	
	protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
			int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
		this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
	}
	
	protected Recycler(int maxCapacityPerThread, int interval, int chunkSize) {
		this(maxCapacityPerThread, interval, chunkSize, true, null, false);
	}
	
	protected Recycler(int maxCapacityPerThread, int interval, int chunkSize, boolean unguarded) {
		this(maxCapacityPerThread, interval, chunkSize, true, null, unguarded);
	}
	
	protected Recycler(int maxCapacityPerThread, int interval, int chunkSize, Thread owner, boolean unguarded) {
		this(maxCapacityPerThread, interval, chunkSize, false, owner, unguarded);
	}
	
	@SuppressWarnings("unchecked")
	private Recycler(int maxCapacityPerThread, int ratio, int chunkSize, boolean useThreadLocalStorage,
			Thread owner, boolean unguarded) {
		final int interval = Math.max(0, ratio);
		if (maxCapacityPerThread <= 0) {
			maxCapacityPerThread = 0;
			chunkSize = 0;
		} else {
			maxCapacityPerThread = Math.max(4, maxCapacityPerThread);
			chunkSize = Math.max(2, Math.min(chunkSize, maxCapacityPerThread >> 1));
		}
		if (maxCapacityPerThread > 0 && useThreadLocalStorage) {
			final int finalMaxCapacityPerThread = maxCapacityPerThread;
			final int finalChunkSize = chunkSize;
			threadLocalPool = new FastThreadLocal<LocalPool<?, T>> () {
				@Override
				protected LocalPool<?, T> initialValue() {
					return unguarded ? new UnguardedLocalPool<>(finalMaxCapacityPerThread, interval, finalChunkSize) :
						new GuardedLocalPool<>(finalMaxCapacityPerThread, interval, finalChunkSize);
				}
				
				@Override
				protected void onRemoval(LocalPool<?, T> value) throws Exception {
					super.onRemoval(value);
					MessagePassingQueue<?> handles = value.pooledHandles;
					value.pooledHandles = null;
					value.owner = null;
					if (handles != null) {
						handles.clear();
					}
				}
			};
			localPool = null;
		} else {
			threadLocalPool = null;
			if (maxCapacityPerThread == 0) {
				localPool = (LocalPool<?, T>) NOOP_LOCAL_POOL;
			} else {
				Objects.requireNonNull(owner, "owner");
				localPool = unguarded ? new UnguardedLocalPool<>(owner, maxCapacityPerThread, interval, chunkSize) :
					new GuardedLocalPool<>(owner, maxCapacityPerThread, interval, chunkSize);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public final T get()  {
		if (localPool != null) {
			return localPool.getWith(this);
		} else {
			if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
				return newObject((Handle<T>) NOOP_HANDLE);
			}
			return threadLocalPool.get().getWith(this);
		}
	}
	
	public static void unpinOwner(Recycler<?> recycler) {
		if (recycler.localPool != null) {
			recycler.localPool.owner = null;
		}
	}
	
	public final boolean recycle(T o, Handle<T> handle) {
		if (handle == NOOP_HANDLE) {
			return false;
		}
		handle.recycle(o);
		return true;
	}
	
	@VisibleForTesting
	final int threadLocalSize() {
		if (localPool != null) {
			return localPool.size();
		} else {
			if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
				return 0;
			}
			final LocalPool<?, T> pool = threadLocalPool.getIfExists();
			if (pool == null) {
				return 0;
			}
			return pool.size();
		}
	}
	
	protected abstract T newObject(Handle<T> handle);
	
	@SuppressWarnings("ClassNameSameAsAncestorName")
	public interface Handle<T> extends ObjectPool.Handle<T> {}
	
	@UnstableApi
	public abstract static class EnhancedHandle<T> implements Handle<T> {
		public abstract void unguardedRecycle(Object object);
		
		private EnhancedHandle() {
			
		}
	}
	
	@SuppressWarnings("unchecked")
	private static final class DefaultHandle<T> extends EnhancedHandle<T> {
		private static final int STATE_CLAIMED = 0;
		private static final int STATE_AVAILABLE = 1;
		private static final AtomicIntegerFieldUpdater<DefaultHandle<?>> STATE_UPDATER;
		static {
			AtomicIntegerFieldUpdater<?> updater = AtomicIntegerFieldUpdater.newUpdater(DefaultHandle.class, "state");
			STATE_UPDATER = (AtomicIntegerFieldUpdater<DefaultHandle<?>>) updater;
		}
		
		private volatile int state;
		private final GuardedLocalPool<T> localPool;
		private T value;
		
		DefaultHandle(GuardedLocalPool<T> localPool) {
			this.localPool = localPool;
		}
		
		@Override
		public void recycle(Object object) {
			if (object != value) {
				throw new IllegalArgumentException("object does not belong to handle");
			}
			toAvailable();
			localPool.release(this);
		}
		
		@Override
		public void unguardedRecycle(Object object) {
			if (object != value) {
				throw new IllegalArgumentException("object does not belong to handle");
			}
			unguardedToAvailable();
			localPool.release(this);
		}
		
		T claim() {
			assert state == STATE_AVAILABLE;
			STATE_UPDATER.lazySet(this, STATE_CLAIMED);
			return value;
		}
		
		void set(T value) {
			this.value = value;
		}
		
		private void toAvailable() {
			int prev = STATE_UPDATER.getAndSet(this, STATE_AVAILABLE);
			if (prev == STATE_AVAILABLE) {
				throw new IllegalStateException("Object has been recycled already.");
			}
		}
		
		private void unguardedToAvailable() {
			int prev = state;
			if (prev == STATE_AVAILABLE) {
				throw new IllegalStateException("Object has been recycled already.");
			}
			STATE_UPDATER.lazySet(this, STATE_AVAILABLE);
		}
	}
	
	private static final class GuardedLocalPool<T> extends LocalPool<DefaultHandle<T>, T> {
		GuardedLocalPool(int maxCapacity) {
			super(maxCapacity);
		}
		
		GuardedLocalPool(Thread owner, int maxCapacity, int rationInterval, int chunkSize) {
			super(owner, maxCapacity, rationInterval, chunkSize);
		}
		
		GuardedLocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
			super(maxCapacity, ratioInterval, chunkSize);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public T getWith(Recycler<T> recycler) {
			DefaultHandle<T> handle = acquire();
			T obj;
			if (handle == null) {
				handle = canAllocatePooled() ? new DefaultHandle<>(this) : null;
				if (handle != null) {
					obj = recycler.newObject(handle);
					handle.set(obj);
				} else {
					obj = recycler.newObject((Handle<T>) NOOP_HANDLE);
				}
			} else {
				obj = handle.claim();
			}
			return obj;
		}
	}
	
	private static final class UnguardedLocalPool<T> extends LocalPool<T, T> {
		private final EnhancedHandle<T> handle;
		
		UnguardedLocalPool(int maxCapacity) {
			super(maxCapacity);
			handle = maxCapacity == 0 ? null : new LocalPoolHandle<>(this);
		}
		
		UnguardedLocalPool(Thread owner, int maxCapacity, int ratioInterval, int chunkSize) {
			super(owner, maxCapacity, ratioInterval, chunkSize);
			handle = new LocalPoolHandle<>(this);
		}
		
		UnguardedLocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
			super(maxCapacity, ratioInterval, chunkSize);
			handle = new LocalPoolHandle<>(this);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public T getWith(Recycler<T> recycler) {
			T obj = acquire();
			if (obj == null) {
				obj = recycler.newObject(canAllocatePooled() ? handle : (Handle<T>) NOOP_HANDLE);
			}
			return obj;
		}
	}
	
	private abstract static class LocalPool<H, T> {
		private final int ratioInterval;
		private final H[] batch;
		private int batchSize;
		private Thread owner;
		private MessagePassingQueue<H> pooledHandles;
		private int ratioCounter;
		
		LocalPool(int maxCapacity) {
			this.ratioInterval = maxCapacity == 0 ? -1 : 0;
			this.owner = null;
			batch = null;
			batchSize = 0;
			pooledHandles = createExternalMcPool(maxCapacity);
			ratioCounter = 0;
		}
		
		@SuppressWarnings("unchecked")
		LocalPool(Thread owner, int maxCapacity, int ratioInterval, int chunkSize) {
			this.ratioInterval = ratioInterval;
			this.owner = owner;
			batch = owner != null ? (H[]) new Object[chunkSize] : null;
			batchSize = 0;
			pooledHandles = createExternalScPool(chunkSize, maxCapacity);
			ratioCounter = ratioInterval;
		}
		
		@SuppressWarnings("unchecked")
		private static <H> MessagePassingQueue<H> createExternalMcPool(int maxCapacity) {
			if (maxCapacity == 0) {
				return null;
			}
			if (BLOCKING_POOL) {
				return new BlockingMessageQueue<>(maxCapacity);
			}
			return (MessagePassingQueue<H>) newFixedMpmcQueue(maxCapacity);
		}
		
		@SuppressWarnings("unchecked")
		private static <H> MessagePassingQueue<H> createExternalScPool(int chunkSize, int maxCapacity) {
			if (maxCapacity == 0) {
				return null;
			}
			if (BLOCKING_POOL) {
				return new BlockingMessageQueue<>(maxCapacity);
			}
			return (MessagePassingQueue<H>) newMpscQueue(chunkSize, maxCapacity);
		}
		
		LocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
			this(!BATCH_FAST_TL_ONLY || FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()
					? Thread.currentThread() : null, maxCapacity, ratioInterval, chunkSize);
		}
		
		protected final H acquire() {
			int size = batchSize;
			if (size == 0) {
				final MessagePassingQueue<H> handles = pooledHandles;
				if (handles == null) {
					return null;
				}
				return handles.relaxedPoll();
			}
			int top = size - 1;
			final H h = batch[top];
			batchSize = top;
			batch[top] = null;
			return h;
		}
		
		protected final void release(H handle) {
			Thread owner = this.owner;
			if (owner != null && Thread.currentThread() == owner && batchSize < batch.length) {
				batch[batchSize] = handle;
				batchSize++;
			} else if (owner != null && isTerminated(owner)) {
				pooledHandles = null;
				this.owner = null;
			} else {
				MessagePassingQueue<H> handles = pooledHandles;
				if (handles != null) {
					handles.relaxedOffer(handle);
				}
			}
		}
		
		private static boolean isTerminated(Thread owner) {
			return PlatformDependent.isJ9Jvm() ? !owner.isAlive() : owner.getState() == Thread.State.TERMINATED;
		}
		
		boolean canAllocatePooled() {
			if (ratioInterval < 0) {
				return false;
			}
			if (ratioInterval == 0) {
				return true;
			}
			if (++ratioCounter >= ratioInterval) {
				ratioCounter = 0;
				return true;
			}
			return false;
		}
		
		abstract T getWith(Recycler<T> recycler);
		
		int size() {
			MessagePassingQueue<H> handles = pooledHandles;
			final int externalSize = handles != null ? handles.size() : 0;
			return externalSize + (batch != null ? batchSize : 0);
		}
	}
	
	private static final class BlockingMessageQueue<T> implements MessagePassingQueue<T> {
		private final Queue<T> deque;
		private final int maxCapacity;
		
		BlockingMessageQueue(int maxCapacity) {
			this.maxCapacity = maxCapacity;
			deque = new ArrayDeque<T>();
		}
		
		@Override
		public synchronized boolean offer(T e) {
			if (deque.size() == maxCapacity) {
				return false;
			}
			return deque.offer(e);
		}
		
		@Override
		public synchronized T poll() {
			return deque.poll();
		}
		
		@Override
		public synchronized T peek() {
			return deque.peek();
		}
		
		@Override
		public synchronized int size() {
			return deque.size();
		}
		
		@Override
		public synchronized void clear() {
			deque.clear();
		}
		
		@Override
		public synchronized boolean isEmpty() {
			return deque.isEmpty();
		}
		
		@Override
		public int capacity() {
			return maxCapacity;
		}
		
		@Override
		public boolean relaxedOffer(T e) {
			return offer(e);
		}
		
		@Override
		public T relaxedPoll() {
			return poll();
		}
		
		@Override
		public T relaxedPeek() {
			return peek();
		}
		
		@Override
		public int drain(Consumer<T> c, int limit) {
			T obj;
			int i = 0;
			for (; i < limit && (obj = poll()) != null; i++) {
				c.accept(obj);
			}
			return i;
		}
		
		@Override
		public int fill(Supplier<T> s) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int fill(Supplier<T> s, int limit) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int drain(Consumer<T> c) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void drain(Consumer<T> c, WaitStrategy wait, ExitCondition exit) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void fill(Supplier<T> s, WaitStrategy wait, ExitCondition exit) {
			throw new UnsupportedOperationException();
		}

	}
} 
