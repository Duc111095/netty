package netty.common.util.concurrent;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jetbrains.annotations.Async.Schedule;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.ThreadExecutorMap;
import netty.common.util.internal.ThrowableUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class GlobalEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(GlobalEventExecutor.class);
	
	private static final long SCHEDULE_QUIET_PERIOD_INTERVAL;
	
	static {
		int quietPeriod = SystemPropertyUtil.getInt("io.netty.globalEventExecutor.quietPeriodSeconds", 1);
		if (quietPeriod <= 0) {
			quietPeriod = 1;
		}
		logger.debug("-Dio.netty.globalEventExecutor.quietPeriodSeconds: {}", quietPeriod);
		
		SCHEDULE_QUIET_PERIOD_INTERVAL = TimeUnit.SECONDS.toNanos(quietPeriod);
	}
	
	public static final GlobalEventExecutor INSTANCE = new GlobalEventExecutor();
	
	final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>();
	final ScheduledFutureTask<Void> quietPeriodTask = new ScheduledFutureTask<Void>(
			this, Executors.<Void>callable(new Runnable() {
				@Override
				public void run() {
					
				}
			}, null), 
				deadlineNanos(getCurrentTimeNanos(), SCHEDULE_QUIET_PERIOD_INTERVAL),
				-SCHEDULE_QUIET_PERIOD_INTERVAL
	);
	
	final ThreadFactory threadFactory;
	private final TaskRunner taskRunner = new TaskRunner();
	private final AtomicBoolean started = new AtomicBoolean();
	volatile Thread thread;
	
	private final Future<?> terminationFuture;
	
	private GlobalEventExecutor() {
		scheduledFromEventLoop(quietPeriodTask);
		threadFactory = ThreadExecutorMap.apply(new DefaultThreadFactory(
				DefaultThreadFactory.toPoolName(getClass()), false, Thread.NORM_PRIORITY, null), this);
		
		UnsupportedOperationException terminationFailure = new UnsupportedOperationException();
		ThrowableUtil.unknownStackTrace(terminationFailure, GlobalEventExecutor.class, "terminationFuture");
		terminationFuture = new FailedFuture<Object>(this, terminationFailure);
	}
	
	Runnable takeTask() {
		BlockingQueue<Runnable> taskQueue = this.taskQueue;
		for (;;) {
			ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
			if (scheduledTask == null) {
				Runnable task = null;
				try {
					task = taskQueue.take();
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
					return task;
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void fetchFromScheduledTaskQueue() {
		long nanoTime = getCurrentTimeNanos();
		ScheduledFutureTask scheduledTask;
		while ((scheduledTask = (ScheduledFutureTask) pollScheduledTask(nanoTime)) != null) {
			if (scheduledTask.isCancelled()) {
				continue;
			}
			taskQueue.add(scheduledTask);
		}
	}
	
	public int pendingTasks() {
		return taskQueue.size();
	}
	
	private void addTask(Runnable task) {
		taskQueue.add(ObjectUtil.checkNotNull(task, "task"));
	}
	
	@Override
	public boolean inEventLoop(Thread thread) {
		return thread == this.thread;
	}
	
	@Override
	public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		return terminationFuture();
	}
	
	@Override
	public Future<?> terminationFuture() {
		return terminationFuture;
	}
	
	@Override
	public void shutdown() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean isShuttingDown() {
		return false;
	}
	
	@Override
	public boolean isShutdown() {
		return false;
	}
	
	@Override
	public boolean isTerminated() {
		return false;
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) {
		return false;
	}
	
	public boolean awaitInactivity(long timeout, TimeUnit unit) throws InterruptedException {
		ObjectUtil.checkNotNull(unit, "unit");
		
		final Thread thread = this.thread;
		if (thread == null) {
			throw new IllegalStateException("thread was not started");
		}
		thread.join(unit.toMillis(timeout));
		return !thread.isAlive();
	}
	
	@Override
	public void execute(Runnable task) {
		execute0(task);
	}
	
	private void execute0(@Schedule Runnable task) {
		addTask(ObjectUtil.checkNotNull(task, "task"));
		if (!inEventLoop()) {
			startThread();
		}
	}
	
	private void startThread() {
		if (started.compareAndSet(false, true)) {
			final Thread callingThread = Thread.currentThread();
			ClassLoader parentCCL = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
				@Override
				public ClassLoader run() {
					return callingThread.getContextClassLoader();
				}
			});
			setContextClassLoader(callingThread, null);
			try {
				final Thread t = threadFactory.newThread(taskRunner);
				setContextClassLoader(t, null);
				thread = t;
				t.start();
			} finally {
				setContextClassLoader(callingThread, parentCCL);
			}
		}
	}
	
	private static void setContextClassLoader(final Thread t, final ClassLoader cl) {
		AccessController.doPrivileged(new PrivilegedAction<Void>() {
			@Override
			public Void run() {
				t.setContextClassLoader(cl);
				return null;
			}
		});
	}
	
	final class TaskRunner implements Runnable {
		@Override
		public void run() {
			for (;;) {
				Runnable task = takeTask();
				if (task != null) {
					try {
						runTask(task);
					} catch (Throwable t) {
						logger.warn("Unexpected exception form the global event executor: ", t);
					}
					
					if (task != quietPeriodTask) {
						continue;
					}
				}
				
				Queue<ScheduledFutureTask<?>> scheduledTaskQueue = GlobalEventExecutor.this.scheduledTaskQueue;
				if (taskQueue.isEmpty() && (scheduledTaskQueue == null || scheduledTaskQueue.size() == 1)) {
					boolean stopped = started.compareAndSet(true, false);
					assert stopped;
					
					if (taskQueue.isEmpty()) {
						break;
					}
					
					if (!started.compareAndSet(false, true)) {
						break;
					}
				}
			}
		}
	}
}
