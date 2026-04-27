package netty.common.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import netty.common.util.internal.DefaultPriorityQueue;
import netty.common.util.internal.PriorityQueueNode;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {

	private long id;
	
	private long deadlineNanos;
	private final long periodNanos;
	
	private int queueIndex = INDEX_NOT_IN_QUEUE;
	
	ScheduledFutureTask(AbstractScheduledEventExecutor executor,
			Runnable runnable, long nanoTime) {
		super(executor, runnable);
		deadlineNanos = nanoTime;
		periodNanos = 0;
	}
	
	ScheduledFutureTask(AbstractScheduledEventExecutor executor,
			Runnable runnable, long nanoTime, long period) {
		
		super(executor, runnable);
		deadlineNanos = nanoTime;
		periodNanos = validatePeriod(period);
	}
	
	ScheduledFutureTask(AbstractScheduledEventExecutor executor,
			Callable<V> callable, long nanoTime, long period) {
		
		super(executor, callable);
		deadlineNanos = nanoTime;
		periodNanos = validatePeriod(period);
	}
	
	ScheduledFutureTask(AbstractScheduledEventExecutor executor,
			Callable<V> callable, long nanoTime) {
		
		super(executor, callable);
		deadlineNanos = nanoTime;
		periodNanos = 0;
	}
	
	private static long validatePeriod(long period) {
		if (period == 0) {
			throw new IllegalArgumentException("period: 0 (expected: != 0)");
		}
		return period;
	}
	
	ScheduledFutureTask<V> setId(long id) {
		if (this.id == 0L) {
			this.id = id;
		}
		return this;
	}
	
	long getId() {
		return id;
	}
	
	@Override
	protected EventExecutor executor() {
		return super.executor();
	}
	
	public long deadlineNanos() {
		return deadlineNanos;
	}
	
	void setConsumed() {
		if (periodNanos == 0) {
			assert scheduledExecutor().getCurrentTimeNanos() >= deadlineNanos;
			deadlineNanos = 0L;
		}
	}
	
	public long delayNanos() {
		if (deadlineNanos == 0L) {
			return 0L;
		}
		return delayNanos(scheduledExecutor().getCurrentTimeNanos());
	}
	
	static long deadlineToDelayNanos(long currentTimeNanos, long deadlineNanos) {
		return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos - currentTimeNanos);
	}
	
	public long delayNanos(long currentTimeNanos) {
		return deadlineToDelayNanos(currentTimeNanos, deadlineNanos);
	}
	
	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
	}
	
	@Override
	public int compareTo(Delayed o) {
		if (this == o) {
			return 0;
		}
		
		ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
		long d = deadlineNanos() - that.deadlineNanos();
		if (d < 0) {
			return -1;
		} else if (d > 0) {
			return 1;
		} else if (id < that.id) {
			return -1;
		} else {
			assert id != that.id;
			return 1;
		}
	}
	
	@Override
	public void run() {
		assert executor().inEventLoop();
		try {
			if (delayNanos() > 0L) {
				if (isCancelled()) {
					scheduledExecutor().scheduledTaskQueue().removeTyped(this);
				} else {
					scheduledExecutor().scheduledFromEventLoop(this);
				}
				return;
			}
			if (periodNanos == 0) {
				if (setUncancellableInternal()) {
					V result = runTask();
					setSuccessInternal(result);
				}
			} else {
				if (!isCancelled()) {
					runTask();
					if (!executor().isShutdown()) {
						if (periodNanos > 0) {
							deadlineNanos += periodNanos;
						} else {
							deadlineNanos = scheduledExecutor().getCurrentTimeNanos() - periodNanos;
						}
						if (!isCancelled()) {
							scheduledExecutor().scheduledFromEventLoop(this);
						}
					}
				}
			} 
		} catch (Throwable cause) {
			setFailureInternal(cause);
		}
	}
	
	private AbstractScheduledEventExecutor scheduledExecutor() {
		return (AbstractScheduledEventExecutor) executor();
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean cancelled = super.cancel(mayInterruptIfRunning);
		if (cancelled) {
			scheduledExecutor().removeScheduled(this);
		}
		return cancelled;
	}
	
	boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
		return super.cancel(mayInterruptIfRunning);
	}
	
	@Override
	protected StringBuilder toStringBuilder() {
		StringBuilder buf = super.toStringBuilder();
		buf.setCharAt(buf.length() - 1, ',');
		
		return buf.append(" deadline: ")
				.append(deadlineNanos)
				.append(", period: ")
				.append(periodNanos)
				.append(')');
	}
	
	@Override
	public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
		return queueIndex;
	}
	
	@Override
	public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
		queueIndex = i;
	}
}
