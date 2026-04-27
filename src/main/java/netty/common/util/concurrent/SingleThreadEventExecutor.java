package netty.common.util.concurrent;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.ThreadExecutorMap;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.Async.Schedule;


public abstract class SingleThreadEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {
	
	static final int DEFAULT_MAX_PENDING_EXECUTOR_TASKS = Math.max(16, 
			SystemPropertyUtil.getInt("io.netty.eventexecutor.maxPendingTasks", Integer.MAX_VALUE));
	
	private static final InternalLogger logger = 
			InternalLoggerFactory.getInstance(SingleThreadEventExecutor.class);
	
	private static final int ST_NOT_STARTED = 1;
	private static final int ST_SUSPENDING = 2;
	private static final int ST_SUSPENDED = 3;
	private static final int ST_STARTED = 4;
	private static final int ST_SHUTTING_DOWN = 5;
	private static final int ST_SHUTDOWN = 6;
	private static final int ST_TERMINATED = 7;
	
	private static final Runnable NOOP_TASK = new Runnable() {
		@Override
		public void run() {
			
		}
	};
	
	private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> STATE_UPDATER = 
			AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "state");
	private static final AtomicReferenceFieldUpdater<SingleThreadEventExecutor, ThreadProperties> PROPERTIES_UPDATER = 
			AtomicReferenceFieldUpdater.newUpdater(
					SingleThreadEventExecutor.class, ThreadProperties.class, "threadProperties");
	private static final AtomicLongFieldUpdater<SingleThreadEventExecutor> ACCUMULATED_ACTIVE_TIME_NANOS_UPDATER =
			AtomicLongFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "accumulatedActiveTimeNanos");
	private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> CONSECUTIVE_IDLE_CYCLES_UPDATER = 
			AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "consecutiveIdleCycles");
	private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> CONSECUTIVE_BUSY_CYCLES_UPDATER = 
			AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "consecutiveBusyCycles");
	private final Queue<Runnable> taskQueue;

	private volatile Thread thread;
	private volatile ThreadProperties threadProperties;
	private final Executor executor;
	private volatile boolean interrupted;
	
	private final Lock processingLock = new ReentrantLock();
	private final CountDownLatch threadLock = new CountDownLatch(1);
	private final Set<Runnable> shutdownHooks = new LinkedHashSet<Runnable>();
	private final boolean addTaskWakesUp;
	private final int maxPendingTasks;
	private final RejectedExecutionHandler rejectedExecutionHandler;
	private final boolean supportSuspension;
	
	private volatile long accumulatedActiveTimeNanos;
	private volatile long lastActivityTimeNanos;
	private volatile int consecutiveIdleCycles;
	
	private volatile int consecutiveBusyCycles;
	private long lastExecutionTime;
	
	private volatile int state = ST_NOT_STARTED;
	
	private volatile long gracefulShutdownQuietPeriod;
	private volatile long gracefulShutdownTimeout;
	private long gracefulShutdownStartTime;
	
	private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);
	
	protected SingleThreadEventExecutor(
			EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp) {
		this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp);
	}
	
	protected SingleThreadEventExecutor(
			EventExecutorGroup parent, ThreadFactory threadFactory,
			boolean addTaskWakesUp, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
		this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, maxPendingTasks, rejectedHandler);
	}
	
	protected SingleThreadEventExecutor(
			EventExecutorGroup parent, ThreadFactory threadFactory,
			boolean addTaskWakesUp, boolean supportSuspension,
			int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
		this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, supportSuspension, 
				maxPendingTasks, rejectedHandler);
	}
	
	protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp) {
		this(parent, executor, addTaskWakesUp, DEFAULT_MAX_PENDING_EXECUTOR_TASKS, RejectedExecutionHandlers.reject());
	}
	
	protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, 
			boolean addTaskWakesUp, int maxPendingTasks,
			RejectedExecutionHandler rejectedHandler) {
		this(parent, executor, addTaskWakesUp, false, maxPendingTasks, rejectedHandler);
	}
	
	protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
			boolean addTaskWakesUp, boolean supportSuspension,
			int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
		super(parent);
		this.addTaskWakesUp = addTaskWakesUp;
		this.supportSuspension = supportSuspension;
		this.maxPendingTasks = Math.max(16, maxPendingTasks);
		this.executor = ThreadExecutorMap.apply(executor, this);
		rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectHandler");
		lastActivityTimeNanos = ticker().nanoTime();
	}
	
	protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
			boolean addTaskWakesUp, Queue<Runnable> taskQueue,
			RejectedExecutionHandler rejectedHandler) {
		this(parent, executor, addTaskWakesUp, false, taskQueue, rejectedHandler);
	}
	
	protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
			boolean addTaskWakesUp, boolean supportSuspension,
			Queue<Runnable> taskQueue, RejectedExecutionHandler rejectedHandler) {
		super(parent);
		this.addTaskWakesUp = addTaskWakesUp;
		this.supportSuspension = supportSuspension;
		this.maxPendingTasks = DEFAULT_MAX_PENDING_EXECUTOR_TASKS;
		this.executor = ThreadExecutorMap.apply(executor, this);
		taskQueue = newTaskQueue(this.maxPendingTasks);
		rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
		lastActivityTimeNanos = ticker().nanoTime();
	}
	
	protected Queue<Runnable> newTaskQueue() {
		return newTaskQueue(maxPendingTasks);
	}
	
	protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
		return new LinkedBlockingQueue<Runnable>(maxPendingTasks);
	}
	
	protected void interruptThread() {
		Thread currentThread = thread;
		if (currentThread == null) {
			interrupted = true;
		} else  {
			currentThread.interrupt();
		}
	}
	
	protected Runnable pollTask() {
		assert inEventLoop();
		return pollTaskFrom(taskQueue);
	}
	
	protected static Runnable pollTaskFrom(Queue<Runnable> taskQueue) {
		for (;;) {
			Runnable task = taskQueue.poll();
			if (task != WAKEUP_TASK) {
				return task;
			}
		}
	}
	
	protected Runnable takeTask() {
		assert inEventLoop();
		if (!(taskQueue instanceof BlockingQueue)) {
			throw new UnsupportedOperationException();
		}
		
		BlockingQueue<Runnable> taskQueue = (BlockingQueue<Runnable>) this.taskQueue;
		for (;;) {
			ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
			if (scheduledTask == null) {
				Runnable task = null;
				try {
					task = taskQueue.take();
					if (task == WAKEUP_TASK) {
						task = null;
					}
				} catch (InterruptedException e) {
					
				}
				return task;
			} else {
				long delayNanos = scheduledTask.delayNanos();
				Runnable task = null;
				if (delayNanos > 0) {
					try {
						task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
					} catch (InterruptedException e) {
						return null;
					}
				}
				if (task == null) {
					fetchFromScheduledTaskQueue();
					task = taskQueue.poll();
				}
				
				if (task != null) {
					if (task == WAKEUP_TASK) {
						return null;
					}
					return task;
				}
			}
		}
	}
	
	private boolean fetchFromScheduledTaskQueue() {
		return fetchFromScheduledTaskQueue(taskQueue);
	}
	
	private boolean executeExpiredScheduledTasks() {
		if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
			return false;
		}
		long nanoTime = getCurrentTimeNanos();
		Runnable scheduledTask = pollScheduledTask(nanoTime);
		if (scheduledTask == null) {
			return false;
		}
		do {
			safeExecute(scheduledTask);
		} while ((scheduledTask = pollScheduledTask(nanoTime)) != null);
		return true;
	}
	
	protected Runnable peekTask() {
		assert inEventLoop();
		return taskQueue.peek();
	}
	
	protected boolean hasTasks() {
		assert inEventLoop();
		return !taskQueue.isEmpty();
	}
	
	public int pendingTasks() {
		return taskQueue.size();
	}
	
	protected void addTask(Runnable task) {
		ObjectUtil.checkNotNull(task, "task");
		if (!offerTask(task)) {
			reject(task);
		}
	}
	
	final boolean offerTask(Runnable task) {
		if (isShutdown()) {
			reject();
		}
		return taskQueue.offer(task);
	}
	
	protected boolean removeTask(Runnable task) {
		return taskQueue.remove(ObjectUtil.checkNotNull(task, "task"));
	}
	
	protected boolean runAllTasks() {
		assert inEventLoop();
		boolean fetchedAll;
		boolean ranAtLeastOne = false;
		
		do {
			fetchedAll = fetchFromScheduledTaskQueue(taskQueue);
			if (runAllTasksFrom(taskQueue)) {
				ranAtLeastOne = true;
			}
		} while (!fetchedAll);
		
		if (ranAtLeastOne) {
			lastExecutionTime = getCurrentTimeNanos();
		}
		afterRunningAllTasks();
		return ranAtLeastOne;
	}	
	
	protected final boolean runScheduledAndExecutorTasks(final int maxDrainAttempts) {
		assert inEventLoop();
		boolean ranAtLeastOneTask;
		int drainAttempt = 0;
		do {
			ranAtLeastOneTask = runExistingTasksFrom(taskQueue) | executeExpiredScheduledTasks();
		} while (ranAtLeastOneTask && ++drainAttempt < maxDrainAttempts);
		
		if (drainAttempt > 0) {
			lastExecutionTime = getCurrentTimeNanos();
		}
		afterRunningAllTasks();
		
		return drainAttempt > 0;
	}
	
	protected final boolean runAllTasksFrom(Queue<Runnable> taskQueue) {
		Runnable task = pollTaskFrom(taskQueue);
		if (task == null) {
			return false;
		}
		for (;;) {
			safeExecute(task);
			task = pollTaskFrom(taskQueue);
			if (task == null) {
				return true;
			}
		}
	}
	
	private boolean runExistingTasksFrom(Queue<Runnable> taskQueue) {
		Runnable task = pollTaskFrom(taskQueue);
		if (task == null) {
			return false;
		}
		int remaining = Math.min(maxPendingTasks, taskQueue.size());
		safeExecute(task);
		
		while (remaining-- > 0 && (task = taskQueue.poll()) != null) {
			safeExecute(task);
		}
		return true;
	}
	
	@SuppressWarnings("NonAtomicOperationOnVolatileField")
	protected boolean runAllTasks(long timeoutNanos) {
		fetchFromScheduledTaskQueue(taskQueue);
		Runnable task = pollTask();
		if (task == null) {
			afterRunningAllTasks();
			return false;
		}
		
		final long deadline = timeoutNanos > 0 ? getCurrentTimeNanos() + timeoutNanos : 0;
		long runTasks = 0;
		long lastExecutionTime;
		
		long workStartTime = ticker().nanoTime();
		for (;;) {
			safeExecute(task);
			runTasks++;
			
			if ((runTasks & 0x3F) == 0) {
				lastExecutionTime = getCurrentTimeNanos();
				if (lastExecutionTime >= deadline) {
					break;
				}
			}
			
			task = pollTask();
			if (task == null) {
				lastExecutionTime = getCurrentTimeNanos();
				break;
			}
		}
		
		long workEndTime = ticker().nanoTime();
		accumulatedActiveTimeNanos += workEndTime - workStartTime;
		lastActivityTimeNanos = workEndTime;
		
		afterRunningAllTasks();
		this.lastExecutionTime = lastExecutionTime;
		return true;
	}
	
	protected void afterRunningAllTasks() {}
	
	protected long delayNanos(long currentTimeNanos) {
		currentTimeNanos -= ticker().initialNanoTime();
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		if (scheduledTask == null) {
			return SCHEDULE_PURGE_INTERVAL;
		}
		return scheduledTask.delayNanos(currentTimeNanos);
	}
	
	protected long deadlineNanos() {
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		if (scheduledTask == null) {
			return getCurrentTimeNanos() + SCHEDULE_PURGE_INTERVAL;
		}
		return scheduledTask.deadlineNanos();
	}
	
	protected void updateLastExecutionTime() {
		long now = getCurrentTimeNanos();
		lastExecutionTime = now;
		lastActivityTimeNanos = now;
	}
	
	protected int getNumOfRegisteredChannels() {
		return -1;
	}
	
	@SuppressWarnings("NonAtomicOperationOnVolatileField")
	protected void reportActiveIoTime(long nanos) {
		assert inEventLoop();
		if (nanos > 0) {
			accumulatedActiveTimeNanos += nanos;
			lastActivityTimeNanos = ticker().nanoTime();
		}
	}
	
	protected long getAndResetAccumulatedActiveTimeNanos() {
		return ACCUMULATED_ACTIVE_TIME_NANOS_UPDATER.getAndSet(this, 0);
	}
	
	protected long getLastActivityTimeNanos() {
		return lastActivityTimeNanos;
	}
	
	protected int getAndIncrementIdleCycles() {
		return CONSECUTIVE_IDLE_CYCLES_UPDATER.getAndIncrement(this);
	}
	
	protected void resetIdleCycles() {
		CONSECUTIVE_IDLE_CYCLES_UPDATER.set(this, 0);
	}
	
	protected int getAndIncrementBusyCycles() {
		return CONSECUTIVE_BUSY_CYCLES_UPDATER.getAndIncrement(this);
	}
	
	protected void resetBusyCycles() {
		CONSECUTIVE_BUSY_CYCLES_UPDATER.set(this, 0);
	}
	
	protected boolean isSuspensionSupported() {
		return supportSuspension;
	}
	
	protected abstract void run();
	
	protected void cleanup() {
		
	}
	
	protected void wakeup(boolean inEventLoop) {
		if (!inEventLoop) {
			taskQueue.offer(WAKEUP_TASK);
		}
	}
	
	@Override
	public boolean inEventLoop(Thread thread) {
		return thread == this.thread;
	}
	
	public void addShutdownHooks(final Runnable task) {
		if (inEventLoop()) {
			shutdownHooks.add(task);
		} else {
			execute(new Runnable() {
				@Override
				public void run() {
					shutdownHooks.add(task);
				}
			});
		}
	}
	
	public void removeShutdownHook(final Runnable task) {
		if (inEventLoop()) {
			shutdownHooks.remove(task);
		} else {
			execute(new Runnable() {
				@Override
				public void run() {
					shutdownHooks.remove(task);
				}
			});
		}
	}
	
	private boolean runShutdownHooks() {
		boolean ran = false;
		while (!shutdownHooks.isEmpty()) {
			List<Runnable> copy = new ArrayList<Runnable>(shutdownHooks);
			shutdownHooks.clear();
			for (Runnable task : copy) {
				try {
					runTask(task);
				} catch (Throwable t) {
					logger.warn("Shutdown hook raised an exception.", t);
				} finally {
					ran = true;
				}
			}
		}
		
		if (ran) {
			lastExecutionTime = getCurrentTimeNanos();
		}
		return ran;
	}
	
	private void shutdown0(long quietPeriod, long timeout, int shutdownState) {
		if (isShuttingDown()) {
			return;
		}
		
		boolean inEventLoop = inEventLoop();
		boolean wakeup;
		int oldState;
		for (;;) {
			if (isShuttingDown()) {
				return;
			}
			int newState;
			wakeup = true;
			oldState = state;
			if (inEventLoop) {
				newState = shutdownState;
			} else {
				switch (oldState) {
				case ST_NOT_STARTED:
				case ST_STARTED:
				case ST_SUSPENDING:
				case ST_SUSPENDED:
					newState = shutdownState;
					break;
				default:
					newState = oldState;
					wakeup = false;
				}
			}
			if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
				break;
			}
		}
		if (quietPeriod != -1) {
			gracefulShutdownQuietPeriod = quietPeriod;
		}
		if (timeout != -1) {
			gracefulShutdownTimeout = timeout;
		}
		if (ensureThreadStarted(oldState)) {
			return;
		}
		
		if (wakeup) {
			taskQueue.offer(WAKEUP_TASK);
			if (!addTaskWakesUp) {
				wakeup(inEventLoop);
			}
		}
	}
	
	@Override
	public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		ObjectUtil.checkPositiveOrZero(quietPeriod, "quietPeriod");
		if (timeout < quietPeriod) {
			throw new IllegalArgumentException(
					"timeout: " + timeout + " (expected >= quietPeriod (" + quietPeriod + "))");
		}
		ObjectUtil.checkNotNull(unit, "unit");
		
		shutdown0(unit.toNanos(quietPeriod), unit.toNanos(timeout), ST_SHUTTING_DOWN);
		return terminationFuture();
	}
	
	@Override
	public Future<?> terminationFuture() {
		return terminationFuture;
	}
	
	@Override
	public void shutdown() {
		shutdown0(-1, -1, ST_SHUTDOWN);
	}
	
	@Override
	public boolean isShuttingDown() {
		return state >= ST_SHUTTING_DOWN;
	}
	
	@Override
	public boolean isShutdown() {
		return state >= ST_SHUTDOWN;
	}
	
	@Override
	public boolean isTerminated() {
		return state == ST_TERMINATED;
	}
	
	@Override
	public boolean isSuspended() {
		int currentState = state;
		return currentState == ST_SUSPENDED || currentState == ST_SUSPENDING;
	}
	
	@Override
	public boolean trySuspend() {
		if (supportSuspension) {
			if (STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_SUSPENDING)) {
				wakeup(inEventLoop());
				return true;
			} else if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_SUSPENDED)) {
				return true;
			}
			int currentState = state;
			return currentState == ST_SUSPENDED || currentState == ST_SUSPENDING;
		}
		return false;
	}
	
	protected boolean canSuspend() {
		return canSuspend(state);
	}
	
	protected boolean canSuspend(int state) {
		assert inEventLoop();
		return supportSuspension && (state == ST_SUSPENDED || state == ST_SUSPENDING)
				&& !hasTasks() && nextScheduledTaskDeadlineNanos() == -1;
	}
	
	protected boolean confirmShutdown() {
		if (!isShuttingDown()) {
			return false;
		}
		
		if (!inEventLoop()) {
			throw new IllegalStateException("must be invoked from an event loop");
		}
		cancelScheduledTasks();
		
		if (gracefulShutdownStartTime == 0) {
			gracefulShutdownStartTime = getCurrentTimeNanos();
		}
		
		if (runAllTasks() || runShutdownHooks()) {
			if (isShutdown()) {
				return true;
			}
			
			if (gracefulShutdownQuietPeriod == 0) {
				return true;
			}
			taskQueue.offer(WAKEUP_TASK);
			return false;
		}
		
		final long nanoTime = getCurrentTimeNanos();
		
		if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
			return true;
		}
		
		if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
			taskQueue.offer(WAKEUP_TASK);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				
			}
			return false;
		}
		
		return true;
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		ObjectUtil.checkNotNull(unit, "unit");
		if (inEventLoop()) {
			throw new IllegalStateException("cannot await termination of the current thread");
		}
		
		threadLock.await(timeout, unit);
		return isTerminated();
	}
	
	@Override
	public void execute(Runnable task) {
		execute0(task);
	}
	
	@Override
	public void lazyExecute(Runnable task) {
		lazyExecute0(task);
	}
	
	private void execute0(@Schedule Runnable task) {
		ObjectUtil.checkNotNull(task, "task");
		execute(task, wakesUpForTask(task));
	}
	
	private void lazyExecute0(@Schedule Runnable task) {
		execute(ObjectUtil.checkNotNull(task, "task"), false);
	}
	
	@Override
	void scheduleRemoveScheduled(final ScheduledFutureTask<?> task) {
		ObjectUtil.checkNotNull(task, "task");
		int currentState = state;
		if (supportSuspension && currentState == ST_SUSPENDED) {
			execute(new Runnable() {
				@Override
				public void run() {
					task.run();
					if (canSuspend(ST_SUSPENDED)) {
						trySuspend();
					}
				}
			}, true);
		} else {
			execute(task, false);
		}
	}
	
	private void execute(Runnable task, boolean immediate) {
		boolean inEventLoop = inEventLoop();
		addTask(task);
		if (!inEventLoop) {
			startThread();
			if (isShutdown()) {
				boolean reject = false;
				try {
					if (removeTask(task)) {
						reject = true;
					}
				} catch (UnsupportedOperationException e) {
					
				}
				if (reject) {
					reject();
				}
			}
		}
		
		if (!addTaskWakesUp && immediate) {
			wakeup(inEventLoop);
		}
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		throwIfInEventLoop("invokeAny");
		return super.invokeAny(tasks);
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
		throws InterruptedException, ExecutionException, TimeoutException {
		throwIfInEventLoop("invokeAny");
		return super.invokeAny(tasks, timeout, unit);
	}
	
	@Override
	public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) 
		throws InterruptedException {
		throwIfInEventLoop("invokeAll");
		return super.invokeAll(tasks);
	}
	
	@Override
	public <T> List<java.util.concurrent.Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		throwIfInEventLoop("invokeAll");
		return super.invokeAll(tasks, timeout, unit);
	}
	
	private void throwIfInEventLoop(String method) {
		if (inEventLoop()) {
			throw new RejectedExecutionException("Calling " + method + " from within the EventLoop is not allowed");
		}
	}
	
	public final ThreadProperties threadProperties() {
		ThreadProperties threadProperties = this.threadProperties;
		if (threadProperties == null) {
			Thread thread = this.thread;
			if (thread == null) {
				assert !inEventLoop();
				submit(NOOP_TASK).syncUninterruptibly();
				thread = this.thread;
				assert thread != null;
			}
			
			threadProperties = new DefaultThreadProperties(thread);
			if (!PROPERTIES_UPDATER.compareAndSet(this, null, threadProperties)) {
				threadProperties = this.threadProperties;
			}
		}
		
		return threadProperties;
	}
	
	protected interface NonWakeupRunnable extends LazyRunnable {}
	
	protected boolean wakesUpForTask(Runnable task) {
		return true;
	}
	
	protected static void reject() {
		throw new RejectedExecutionException("event executor terminated");
	}
	
	protected final void reject(Runnable task) {
		rejectedExecutionHandler.rejected(task, this);
	}
	
	private static final long SCHEDULE_PURGE_INTERVAL = TimeUnit.SECONDS.toNanos(1);
	
	private void startThread() {
		int currentState = state;
		if (currentState == ST_NOT_STARTED || currentState == ST_SUSPENDED) {
			if (STATE_UPDATER.compareAndSet(this, currentState, ST_STARTED)) {
				resetIdleCycles();
				resetBusyCycles();
				boolean success = false;
				try {
					doStartThread();
					success = true;
				} finally {
					if (!success) {
						STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_NOT_STARTED);
					}
				}
			}
		}
	}
	
	private boolean ensureThreadStarted(int oldState) {
		if (oldState == ST_NOT_STARTED || oldState == ST_SUSPENDED) {
			try {
				doStartThread();
			} catch (Throwable cause) {
				STATE_UPDATER.set(this, ST_TERMINATED);
				terminationFuture.tryFailure(cause);
				
				if (!(cause instanceof Exception)) {
					PlatformDependent.throwException(cause);
				}
				return true;
			}
		}
		return false;
	}
	
	private void doStartThread() {
		executor.execute(new Runnable() {
			@Override
			public void run() {
				processingLock.lock();
				assert thread == null;
				thread = Thread.currentThread();
				if (interrupted) {
					thread.interrupt();
					interrupted = false;
				}
				boolean success = false;
				Throwable unexpectedException = null;
				updateLastExecutionTime();
				boolean suspend = false;
				try {
					for (;;) {
						SingleThreadEventExecutor.this.run();
						success = true;
						
						int currentState = state;
						if (canSuspend(currentState)) {
							if (!STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this,
									ST_SUSPENDING, ST_SUSPENDED)) {
								continue;
							}
							
							if (!canSuspend(ST_SUSPENDED) && STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this, 
									ST_SUSPENDED, ST_STARTED)) {
								continue;
							}
							suspend = true;
						}
						break;
					}
				} catch (Throwable t) {
					unexpectedException = t;
					logger.warn("Unexpected exception from an event executor: ", t);
				} finally {
					boolean shutdown = !suspend;
					if (shutdown) {
						for (;;) {
							int oldState = state;
							if (oldState >= ST_SHUTTING_DOWN || STATE_UPDATER.compareAndSet(
									SingleThreadEventExecutor.this, oldState, ST_SHUTTING_DOWN)) {
								break;
							}
						}
						if (success && gracefulShutdownStartTime == 0) {
							if (logger.isErrorEnabled()) {
								logger.error("Buggy " + EventExecutor.class.getSimpleName() + " implementation; " +
										SingleThreadEventExecutor.class.getSimpleName() + ".confirmShutdown() must " + 
										"be called before run() implementation terminates.");
							}
						}
					}
					
					try {
						if (shutdown) {
							for (;;) {
								if (confirmShutdown()) {
									break;
								}
							}
							
							for (;;) {
								int currentState = state;
								if (currentState >= ST_SHUTDOWN || STATE_UPDATER.compareAndSet(
										SingleThreadEventExecutor.this, currentState, ST_SHUTDOWN)) {
									break;
								}
							}
							
							confirmShutdown();
						}
					} finally {
						try {
							if (shutdown) {
								try {
									cleanup();
								} finally {
									FastThreadLocal.removeAll();
									
									STATE_UPDATER.set(SingleThreadEventExecutor.this, ST_TERMINATED);
									threadLock.countDown();
									int numUserTasks = drainTasks();
									if (numUserTasks > 0 && logger.isWarnEnabled()) {
										logger.warn("An event executor terminated with " +
												"non-empty task queue(" + numUserTasks + ')');
									}
									if (unexpectedException == null) {
										terminationFuture.setSuccess(null);
									} else {
										terminationFuture.setFailure(unexpectedException);
									}
								}
							} else {
								FastThreadLocal.removeAll();
								threadProperties = null;
							}
						} finally {
							thread = null;
							processingLock.unlock();
						}
					}
				}
			}
		});
	}
	
	final int drainTasks() {
		int numTasks = 0;
		for (;;) {
			Runnable runnable = taskQueue.poll();
			if (runnable == null ) {
				break;
			}
			if (WAKEUP_TASK != runnable) {
				numTasks++;
			}
		}
		return numTasks;
	}
	
	private static final class DefaultThreadProperties implements ThreadProperties {
		
		private final Thread t;
		
		DefaultThreadProperties(Thread t) {
			this.t = t;
		}
		
		@Override
		public State state() {
			return t.getState();
		}
		
		@Override
		public int priority() {
			return t.getPriority();
		}
		
		@Override
		public boolean isInterrupted() {
			return t.isInterrupted();
		}
		
		@Override
		public boolean isDaemon() {
			return t.isDaemon();
		}
		
		@Override
		public String name() {
			return t.getName();
		}
		
		@Override
		public long id() {
			return t.getId();
		}
		
		@Override
		public StackTraceElement[] stackTrace() {
			return t.getStackTrace();
		}
		
		@Override
		public boolean isAlive() {
			return t.isAlive();
		}
	}
}
