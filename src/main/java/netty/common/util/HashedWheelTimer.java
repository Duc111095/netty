package netty.common.util;

import static netty.common.util.internal.StringUtil.simpleClassName;
import static netty.common.util.internal.ObjectUtil.checkPositive;
import static netty.common.util.internal.ObjectUtil.checkNotNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import netty.common.util.concurrent.ImmediateExecutor;
import netty.common.util.internal.MathUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public class HashedWheelTimer implements Timer {
	
	static final InternalLogger logger = 
			InternalLoggerFactory.getInstance(HashedWheelTimer.class);
	
	private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
	private static final AtomicBoolean WARNED_TOO_MANY_INSTANCES = new AtomicBoolean();
	private static final int INSTANCE_COUNT_LIMIT = 64;
	private static final long MILLISECOND_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
	private static final ResourceLeakDetector<HashedWheelTimer> leakDetector = ResourceLeakDetectorFactory.instance()
			.newResourceLeakDetector(HashedWheelTimer.class, 1);
	
	private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");
	
	private final ResourceLeakTracker<HashedWheelTimer> leak;
	private final Worker worker = new Worker();
	private final Thread workerThread;
	
	public static final int WORKER_STATE_INIT = 0;
	public static final int WORKER_STATE_STARTED = 1;
	public static final int WORKER_STATE_SHUTDOWN = 2;
	private volatile int workerState;
	
	private final long tickDuration;
	private final HashedWheelBucket[] wheel;
	private final int mask;
	private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
	private final Queue<HashedWheelTimeout> timeouts = PlatformDependent.newMpscQueue();
	private final Queue<HashedWheelTimeout> cancelledTimeouts = PlatformDependent.newMpscQueue();
	private final AtomicLong pendingTimeouts = new AtomicLong(0);
	private final long maxPendingTimeouts;
	private final Executor taskExecutor;
	
	private volatile long startTime;
	
	public HashedWheelTimer() {
		this(Executors.defaultThreadFactory());
	}
	
	public HashedWheelTimer(long tickDuration, TimeUnit unit) {
		this(Executors.defaultThreadFactory(), tickDuration, unit);
	}
	
	public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
		this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
	}
	
	public HashedWheelTimer(ThreadFactory threadFactory) {
		this(threadFactory, 100, TimeUnit.MILLISECONDS);
	}
	
	public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
		this(threadFactory, tickDuration, unit, 512);
	}
	
	public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit,
			int ticksPerWheel) {
		this(threadFactory, tickDuration, unit, ticksPerWheel, true);
	}
	
	public HashedWheelTimer(
			ThreadFactory threadFactory,
			long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection) {
		this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection, -1);
	}
	
	public HashedWheelTimer(
			ThreadFactory threadFactory,
			long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
			long maxPendingTimeouts) {
		this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection, 
				maxPendingTimeouts, ImmediateExecutor.INSTANCE);
	}
	
	public HashedWheelTimer(
			ThreadFactory threadFactory,
			long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
			long maxPendingTimeouts, Executor taskExecutor) {
		
		checkNotNull(threadFactory, "threadFactory");
		checkNotNull(unit, "unit");
		checkPositive(tickDuration, "tickDuration");
		checkPositive(ticksPerWheel, "ticksPerWheel");
		this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor");
		
		wheel = createWheel(ticksPerWheel);
		mask = wheel.length - 1;
		
		long duration = unit.toNanos(tickDuration);
		
		if (duration >= Long.MAX_VALUE / wheel.length) {
			throw new IllegalArgumentException(String.format(
					"tickDuration: %d (expected: 0 < tickDuration in nanos < %d", 
					tickDuration, Long.MAX_VALUE / wheel.length));
		}
		
		if (duration < MILLISECOND_NANOS) {
			logger.warn("Configured tickDuration {} smaller than {}, using 1ms.",
					tickDuration, MILLISECOND_NANOS);
			this.tickDuration = MILLISECOND_NANOS;
		} else {
			this.tickDuration = duration;
		}
		
		workerThread = threadFactory.newThread(worker);
		leak = leakDetection || !workerThread.isDaemon() ? leakDetector.track(this) : null;
		this.maxPendingTimeouts = maxPendingTimeouts;
		
		if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
				WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
			reportTooManyInstances();
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		try {
			if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
				INSTANCE_COUNTER.decrementAndGet();
			}
		} finally {
			super.finalize();
		}
	}
	
	private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
		ticksPerWheel = MathUtil.findNextPositivePowerOfTwo(ticksPerWheel);
		
		HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
		for (int i = 0; i < wheel.length; i++) {
			wheel[i] = new HashedWheelBucket();
		}
		return wheel;
	}
	
	public void start() {
		int state = WORKER_STATE_UPDATER.get(this);
		switch (state) {
		case WORKER_STATE_INIT:
			if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
				workerThread.start();
			}
			break;
		case WORKER_STATE_STARTED:
			break;
		case WORKER_STATE_SHUTDOWN:
			throw new IllegalStateException("cannot be started once stopped");
		default:
			throw new Error("Invalid WorkerState: " + state);
		}
		
		while (startTime == 0) {
			try {
				startTimeInitialized.await();
			} catch (InterruptedException ignore) {
				
			}
		}
	}
	
	@Override
	public Set<Timeout> stop() {
		if (Thread.currentThread() == workerThread) {
			throw new IllegalStateException(
					HashedWheelTimer.class.getSimpleName() +
						".stop() cannot be called from " +
							TimerTask.class.getSimpleName());
		}
		
		if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
			if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
				INSTANCE_COUNTER.decrementAndGet();
				if (leak != null) {
					boolean closed = leak.close(this);
					assert closed;
				}
			}
			return Collections.emptySet();
		}
		
		try {
			boolean interrupted = false;
			while (workerThread.isAlive()) {
				workerThread.interrupt();
				try {
					workerThread.join(100);
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}
			
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		} finally {
			INSTANCE_COUNTER.decrementAndGet();
			if (leak != null) {
				boolean closed = leak.close(this);
				assert closed;
			}
		}
		
		Set<Timeout> unprocessed = worker.unprocessedTimeouts();
		Set<Timeout> cancelled = new HashSet<Timeout>(unprocessed.size());
		for (Timeout timeout : unprocessed) {
			if (timeout.cancel()) {
				cancelled.add(timeout);
			}
		}
		return cancelled;
	}
	
	@Override
	public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
		checkNotNull(task, "task");
		checkNotNull(unit, "unit");
		
		long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();
		
		if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
			pendingTimeouts.decrementAndGet();
			throw new RejectedExecutionException("Number of pending timeouts ("
					+ pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
					+ "timeouts (" + maxPendingTimeouts + ")");
		}
		start();
		
		long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
		
		if (delay > 0 && deadline < 0) {
			deadline = Long.MAX_VALUE;
		}
		HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
		timeouts.add(timeout);
		return timeout;
	}
	
	public long pendingTimeouts() {
		return pendingTimeouts.get();
	}
	
	private static void reportTooManyInstances() {
		if (logger.isErrorEnabled()) {
			String resourceType = simpleClassName(HashedWheelTimer.class);
			logger.error("You are creating too many " + resourceType + " instances. "
					+ resourceType + " is a shared resource that must be reused across the JVM, " +
					"so that only a few instances are created");
		}
	}
	
	private final class Worker implements Runnable {
		private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();
		
		private long tick;
		
		@Override
		public void run() {
			startTime = System.nanoTime();
			if (startTime == 0) {
				startTime = 1;
			}
			startTimeInitialized.countDown();
			
			do {
				final long deadline = waitForNextTick();
				if (deadline > 0) {
					int idx = (int) (tick & mask);
					processCancelledTasks();
					HashedWheelBucket bucket = wheel[idx];
					transferTimeoutsToBuckets();
					bucket.expireTimeouts(deadline);
					tick++;
				}
			} while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);
			
			for (HashedWheelBucket bucket : wheel) {
				bucket.clearTimeouts(unprocessedTimeouts);
			}
			for (;;) {
				HashedWheelTimeout timeout = timeouts.poll();
				if (timeout == null) {
					break;
				}
				if (!timeout.isCancelled()) {
					unprocessedTimeouts.add(timeout);
				}
			}
			processCancelledTasks();
		}
		
		private void transferTimeoutsToBuckets() {
			for (int i = 0; i < 100000; i++) {
				HashedWheelTimeout timeout = timeouts.poll();
				if (timeout == null) {
					break;
				}
				if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
					continue;
				}
				
				long calculated = timeout.deadline / tickDuration;
				timeout.remainingRounds =  (calculated - tick) / wheel.length;
				
				final long ticks = Math.max(calculated, tick);
				int stopIndex = (int)(ticks & mask);
				
				HashedWheelBucket bucket = wheel[stopIndex];
				bucket.addTimeout(timeout);
			}
		}
		
		private void processCancelledTasks() {
			for (;;) {
				HashedWheelTimeout timeout = cancelledTimeouts.poll();
				if (timeout == null) {
					break;
				}
				try {
					timeout.removeAfterCancellation();
				} catch (Throwable t) {
					if (logger.isWarnEnabled()) {
						logger.warn("An exception was thrown while process a cancellation task", t);
					}
				}
			}
		}
		
		private long waitForNextTick() {
			long deadline = tickDuration * (tick + 1);
			
			for (;;) {
				final long currentTime = System.nanoTime() - startTime;
				long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;
				
				if (sleepTimeMs <= 0) {
					if (currentTime == Long.MIN_VALUE) {
						return -Long.MAX_VALUE;
					} else {
						return currentTime;
					}
				}
				
				if (PlatformDependent.isWindows()) {
					sleepTimeMs = sleepTimeMs / 10 * 10;
					if (sleepTimeMs == 0) {
						sleepTimeMs = 1;
					}
				}
				
				try {
					Thread.sleep(sleepTimeMs);
				} catch (InterruptedException ignored) {
					if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
						return Long.MIN_VALUE;
					}
				}
			}
		}
		
		public Set<Timeout> unprocessedTimeouts() {
			return Collections.unmodifiableSet(unprocessedTimeouts);
		}
	}
	
	
	private static final class HashedWheelTimeout implements Timeout, Runnable {
		private static final int ST_INIT = 0;
		private static final int ST_CANCELLED = 1;
		private static final int ST_EXPIRED = 2;
		private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER = 
				AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");
		
		private final HashedWheelTimer timer;
		private final TimerTask task;
		private final long deadline;
		
		private volatile int state = ST_INIT;
		
		long remainingRounds;
		HashedWheelTimeout next;
		HashedWheelTimeout prev;
		
		HashedWheelBucket bucket;
		
		HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
			this.timer = timer;
			this.task = task;
			this.deadline = deadline;
		}
		
		@Override
		public Timer timer() {
			return timer;
		}
		
		@Override
		public TimerTask task() {
			return task;
		}
		
		@Override
		public boolean cancel() {
			if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
				return false;
			}
			timer.cancelledTimeouts.add(this);
			return true;
		}
		
		private void remove() {
			HashedWheelBucket bucket = this.bucket;
			if (bucket != null) {
				bucket.remove(this);
			}
			timer.pendingTimeouts.decrementAndGet();
		}
		
		void removeAfterCancellation() {
			remove();
			task.cancelled(this);
		}
		
		public boolean compareAndSetState(int expected, int state) {
			return STATE_UPDATER.compareAndSet(this, expected, state);
		}
		
		public int state() {
			return state;
		}
		
		@Override
		public boolean isCancelled() {
			return state() == ST_CANCELLED;
		}
		
		@Override
		public boolean isExpired() {
			return state() == ST_EXPIRED;
		}
		
		public void expire() {
			if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
				return;
			}
			try {
				remove();
				timer.taskExecutor.execute(this);
			} catch (Throwable t) {
				if (logger.isWarnEnabled()) {
					logger.warn("An exception was thrown while submit " + TimerTask.class.getSimpleName()
							+ " for execution.", t);
				}
			}
		}
		
		@Override
		public void run() {
			try {
				task.run(this);
			} catch (Throwable t) {
				if (logger.isWarnEnabled()) {
					logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
				}
			}
		}
		
		@Override
		public String toString() {
			final long currentTime = System.nanoTime();
			long remaining = deadline - currentTime + timer.startTime;
			
			StringBuilder buf = new StringBuilder(192)
					.append(simpleClassName(this))
					.append('(')
					.append("deadline: ");
			if (remaining > 0) {
				buf.append(remaining)
				.append(" ns later");
			} else if (remaining < 0) {
				buf.append(-remaining)
				.append(" ns ago");
			} else {
				buf.append("now");
			}
			
			if (isCancelled()) {
				buf.append(", cancelled");
			}
			return buf.append(", task: ").append(task()).append(')').toString();
		}
	}
	
	private static final class HashedWheelBucket {
		private HashedWheelTimeout head;
		private HashedWheelTimeout tail;
		
		public void addTimeout(HashedWheelTimeout timeout) {
			assert timeout.bucket == null;
			timeout.bucket = this;
			if (head == null) {
				head = tail = timeout;
			}  else {
				tail.next = timeout;
				timeout.prev = tail;
				tail = timeout;
			}
		}
		
		public void expireTimeouts(long deadline) {
			HashedWheelTimeout timeout = head;
			
			while(timeout != null) {
				HashedWheelTimeout next = timeout.next;
				if (timeout.remainingRounds <= 0) {
					if (timeout.deadline <= deadline) {
						timeout.expire();
					} else {
						throw new IllegalStateException(String.format(
								"timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
					}
				} else if (!timeout.isCancelled()) {
					timeout.remainingRounds--;
				} 
				timeout = next;
 			}
		}
		
		public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
			HashedWheelTimeout prev = timeout.prev;
			HashedWheelTimeout next = timeout.next;
			if (prev != null) {
				prev.next = next;
			}
			if (next != null) {
				next.prev = prev;
			}
			
			if (timeout == head) {
				head = next;
			}
			if (timeout == tail) {
				tail = prev;
			}
			timeout.prev = null;
			timeout.next = null;
			timeout.bucket = null;
			return next;
		}
		
		public void clearTimeouts(Set<Timeout> set) {
			for (;;) {
				HashedWheelTimeout timeout = pollTimeout();
				if (timeout == null) {
					return;
				}
				if (timeout.isExpired() || timeout.isCancelled()) {
					continue;
				}
				set.add(timeout);
			}
		}
		
		private HashedWheelTimeout pollTimeout() {
			HashedWheelTimeout head = this.head;
			if (head == null) {
				return null;
			}
			HashedWheelTimeout next = head.next;
			if (next == null) {
				tail = this.head = null;
			} else {
				this.head = next;
				next.prev = null;
			}
			
			head.next = null;
			head.prev = null;
			head.bucket = null;
			return head;
		}
	}
}
