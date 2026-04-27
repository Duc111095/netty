package netty.common.util.concurrent;

import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import netty.common.util.internal.DefaultPriorityQueue;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PriorityQueue;

public abstract class AbstractScheduledEventExecutor extends AbstractEventExecutor {
	private static final Comparator<ScheduledFutureTask<?>> SCHEDULED_FUTURE_TASK_COMPARATOR =
			new Comparator<ScheduledFutureTask<?>>() {
		@Override
		public int compare(ScheduledFutureTask<?> o1, ScheduledFutureTask<?> o2) {
			return o1.compareTo(o2);
		}
	};
	
	static final Runnable WAKEUP_TASK = new Runnable() {
		@Override
		public void run() {}
	};
	
	PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue;
	
	long nextTaskId;
	
	protected AbstractScheduledEventExecutor() {
		
	}
	
	protected AbstractScheduledEventExecutor(EventExecutorGroup parent) {
		super(parent);
	}
	
	@Override
	public Ticker ticker() {
		return Ticker.systemTicker();
	}
	
	protected long getCurrentTimeNanos() {
		return ticker().nanoTime();
	}
	
	protected static long nanoTime() {
		return Ticker.systemTicker().nanoTime();
	}
	
	static long defaultCurrentTimeNanos() {
		return Ticker.systemTicker().nanoTime();
	}
	
	static long deadlineNanos(long nanoTime, long delay) {
		long deadlineNanos = nanoTime + delay;
		return deadlineNanos < 0 ? Long.MAX_VALUE : deadlineNanos;
	}
	
	protected long delayNanos(long currentTimeNanos, long scheduledPurgeInterval) {
		currentTimeNanos -= ticker().initialNanoTime();
		
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		if (scheduledTask == null) {
			return scheduledPurgeInterval;
		}
		return scheduledTask.delayNanos(currentTimeNanos);
	}
	
	protected static long initialNanoTime() {
		return Ticker.systemTicker().initialNanoTime();
	}
	
	PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue() {
		if (scheduledTaskQueue == null) {
			scheduledTaskQueue = new DefaultPriorityQueue<ScheduledFutureTask<?>> (
					 SCHEDULED_FUTURE_TASK_COMPARATOR,
					 11);
		}
		return scheduledTaskQueue;
	}
	
	private static boolean isNullOrEmpty(Queue<ScheduledFutureTask<?>> queue) {
		return queue == null || queue.isEmpty();
	}
	
	protected void cancelScheduledTasks() {
		assert inEventLoop();
		PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;
		if (isNullOrEmpty(scheduledTaskQueue)) {
			return;
		}
		
		final ScheduledFutureTask<?>[] scheduledTasks =
				scheduledTaskQueue.toArray(new ScheduledFutureTask<?>[0]);
		
		for (ScheduledFutureTask<?> task : scheduledTasks) {
			task.cancelWithoutRemove(false);
		}
		
		scheduledTaskQueue.clearIgnoringIndexes();
	}
	
	protected final Runnable pollScheduledTask() {
		return pollScheduledTask(getCurrentTimeNanos());
	}
	
	protected boolean fetchFromScheduledTaskQueue(Queue<Runnable> taskQueue) {
		assert inEventLoop();
		Objects.requireNonNull(taskQueue, "taskQueue");
		if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
			return true;
		}
		long nanoTime = getCurrentTimeNanos();
		for (;;) {
			@SuppressWarnings("rawtypes")
			ScheduledFutureTask scheduledTask = (ScheduledFutureTask) pollScheduledTask(nanoTime);
			if (scheduledTask == null) {
				return true;
			}
			if (scheduledTask.isCancelled()) {
				continue;
			}
			if (!taskQueue.offer(scheduledTask)) {
				scheduledTaskQueue.add(scheduledTask);
				return false;
			}
		}
	}
	
	protected final Runnable pollScheduledTask(long nanoTime) {
		assert inEventLoop();
		
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		if (scheduledTask == null || scheduledTask.deadlineNanos() - nanoTime > 0) {
			return null;
		}
		scheduledTaskQueue.remove();
		scheduledTask.setConsumed();
		return scheduledTask;
	}
	
	protected final long nextScheduledTaskNano() {
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		return scheduledTask != null ? scheduledTask.delayNanos() : -1;
	}
	
	protected final long nextScheduledTaskDeadlineNanos() {
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		return scheduledTask != null ? scheduledTask.deadlineNanos() : -1;
	}
	
	final ScheduledFutureTask<?> peekScheduledTask() {
		Queue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;
		return scheduledTaskQueue != null ? scheduledTaskQueue.peek() : null;
	}
	
	protected final boolean hasScheduledTasks() {
		ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
		return scheduledTask != null && scheduledTask.deadlineNanos() <= getCurrentTimeNanos();
	}
	
	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		ObjectUtil.checkNotNull(command, "command");
		ObjectUtil.checkNotNull(unit, "unit");
		if (delay < 0) {
			delay = 0;
		}
		validateScheduled0(delay, unit);
		
		return schedule(new ScheduledFutureTask<Void>(
				this,
				command,
				deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
	}
	
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		ObjectUtil.checkNotNull(callable, "callable");
		ObjectUtil.checkNotNull(unit, "unit");
		if (delay < 0) {
			delay = 0;
		}
		validateScheduled0(delay, unit);
		
		return schedule(new ScheduledFutureTask<V>(
				this, callable, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
	}
	
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		ObjectUtil.checkNotNull(command, "command");
		ObjectUtil.checkNotNull(unit, "unit");
		if (initialDelay < 0) {
			throw new IllegalArgumentException(
					String.format("initialDelay: %d (expected: >= 0)", initialDelay));
		}
		if (period <= 0) {
			throw new IllegalArgumentException(
					String.format("period: %d (expected: >0)", period));
		}
		validateScheduled0(initialDelay, unit);
		validateScheduled0(period, unit);
		
		return schedule(new ScheduledFutureTask<Void>(
				this, command, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)), unit.toNanos(period)));
	}
	
	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		ObjectUtil.checkNotNull(command, "command");
		ObjectUtil.checkNotNull(unit, "unit");
		if (initialDelay < 0) {
			throw new IllegalArgumentException(
					String.format("initialDelay: %d (expected: >= 0)", initialDelay));
		}
		if (delay <= 0) {
			throw new IllegalArgumentException(
					String.format("delay: %d (expected: >0)", delay));
		}
		
		validateScheduled0(initialDelay, unit);
		validateScheduled0(delay, unit);
		
		return schedule(new ScheduledFutureTask<Void>(
				this, command, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)), -unit.toNanos(delay)));
	}
	
	private void validateScheduled0(long amount, TimeUnit unit) {
		validateScheduled(amount, unit);
	}
	
	protected void validateScheduled(long amount, TimeUnit unit) {
		
	}
	
	final void scheduledFromEventLoop(final ScheduledFutureTask<?> task) {
		if (task.getId() == 0L) {
			task.setId(++nextTaskId);
		}
		scheduledTaskQueue().add(task);
	}
	
	private <V> ScheduledFuture<V> schedule(final ScheduledFutureTask<V> task) {
		if (inEventLoop()) {
			scheduledFromEventLoop(task);
		} else {
			final long deadlineNanos = task.deadlineNanos();
			if (beforeScheduledTaskSubmitted(deadlineNanos)) {
				execute(task);
			} else {
				lazyExecute(task);
				if (afterScheduledTaskSubmitted(deadlineNanos)) {
					execute(WAKEUP_TASK);
				}
			}
		}
		return task;
	}
	
	final void removeScheduled(final ScheduledFutureTask<?> task) {
		assert task.isCancelled();
		if (inEventLoop()) {
			scheduledTaskQueue().removeTyped(task);
		} else {
			scheduleRemoveScheduled(task);
		}
	}
	
	void scheduleRemoveScheduled(final ScheduledFutureTask<?> task) {
		lazyExecute(task);
	}
	
	protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
		return true;
	}
	
	protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
		return true;
	}
}
