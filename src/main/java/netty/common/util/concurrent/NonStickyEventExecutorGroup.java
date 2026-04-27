package netty.common.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;

public final class NonStickyEventExecutorGroup implements EventExecutorGroup {
	private final EventExecutorGroup group;
	private final int maxTaskExecutePerRun;
	
	public NonStickyEventExecutorGroup(EventExecutorGroup group) {
		this(group, 1024);
	}
	
	public NonStickyEventExecutorGroup(EventExecutorGroup group, int maxTaskExecutePerRun) {
		this.group = verify(group);
		this.maxTaskExecutePerRun = ObjectUtil.checkPositive(maxTaskExecutePerRun, "maxTaskExecutePerRun");
	}
	
	private static EventExecutorGroup verify(EventExecutorGroup group) {
		Iterator<EventExecutor> executors = ObjectUtil.checkNotNull(group, "group").iterator();
		while (executors.hasNext()) {
			EventExecutor executor = executors.next();
			if (executor instanceof OrderedEventExecutor) {
				throw new IllegalArgumentException("EventExecutorGroup " + group 
						+ " contains OrderedEventExecutors: " + executor);
			}
		}
		return group;
	}
	
	private NonStickyOrderedEventExecutor newExecutor(EventExecutor executor) {
		return new NonStickyOrderedEventExecutor(executor, maxTaskExecutePerRun);
	}
	
	@Override
	public boolean isShuttingDown() {
		return group.isShuttingDown();
	}
	
	@Override
	public Future<?> shutdownGracefully() {
		return group.shutdownGracefully();
	}
	
	@Override
	public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		return group.shutdownGracefully(quietPeriod, timeout, unit);
	}
	
	@Override
	public Future<?> terminationFuture() {
		return group.terminationFuture();
	}
	
	@Override
	public void shutdown() {
		group.shutdown();
	}
	
	@Override
	public List<Runnable> shutdownNow() {
		return group.shutdownNow();
	}
	
	@Override
	public EventExecutor next() {
		return newExecutor(group.next());
	}
	
	@Override
	public Iterator<EventExecutor> iterator() {
		final Iterator<EventExecutor> itr = group.iterator();
		return new Iterator<EventExecutor>() {
			@Override
			public boolean hasNext() {
				return itr.hasNext();
			}
			
			@Override
			public EventExecutor next() {
				return newExecutor(itr.next());
			}
			
			@Override
			public void remove() {
				itr.remove();
			}
		};
	}
	
	@Override
	public Future<?> submit(Runnable task) {
		return group.submit(task);
	}
	
	@Override
	public <T> Future<T> submit(Runnable task, T result ){
		return group.submit(task, result);
	}
	
	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return group.submit(task);
	}
	
	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return group.schedule(command, delay, unit);
	}
	
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return group.schedule(callable, delay, unit);
	}
	
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long intialDelay, long period, TimeUnit unit) {
		return group.scheduleAtFixedRate(command, intialDelay, period, unit);
	}
	
	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return group.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}
	
	@Override
	public boolean isShutdown() {
		return group.isShutdown();
	}
	
	@Override
	public boolean isTerminated() {
		return group.isTerminated();
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return group.awaitTermination(timeout, unit);
	}
	
	@Override
	public <T> List<java.util.concurrent.Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return group.invokeAll(tasks);
	}
	
	@Override
	public <T> List<java.util.concurrent.Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		return group.invokeAll(tasks, timeout, unit);
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return group.invokeAny(tasks);
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
			throws InterruptedException, ExecutionException, TimeoutException {
		return group.invokeAny(tasks, timeout, unit);
	}
	
	@Override
	public void execute(Runnable command) {
		group.execute(command);
	}
	
	private static final class NonStickyOrderedEventExecutor extends AbstractEventExecutor
			implements Runnable, OrderedEventExecutor {
		private final EventExecutor executor;
		private final Queue<Runnable> tasks = PlatformDependent.newMpscQueue();
		
		private static final int NONE = 0;
		private static final int SUBMITTED = 1;
		private static final int RUNNING = 2;
		
		private final AtomicInteger state = new AtomicInteger();
		private final int maxTaskExecutePerRun;
		
		private final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();
		
		NonStickyOrderedEventExecutor(EventExecutor executor, int maxTaskExecutePerRun) {
			super(executor);
			this.executor = executor;
			this.maxTaskExecutePerRun = maxTaskExecutePerRun;
		}
		
		@Override
		public void run() {
			if (!state.compareAndSet(SUBMITTED, RUNNING)) {
				return;
			}
			Thread current = Thread.currentThread();
			executingThread.set(current);
			for (;;) {
				int i = 0;
				try {
					for (; i < maxTaskExecutePerRun; i++) {
						Runnable task = tasks.poll();
						if (task == null) {
							break;
						}
						safeExecute(task);
					}
				} finally {
					if (i == maxTaskExecutePerRun) {
						try {
							state.set(SUBMITTED);
							executingThread.compareAndSet(current, null);
							executor.execute(this);
							return;
						} catch (Throwable ignore) {
							executingThread.set(current);
							state.set(RUNNING);
						}
					} else {
						state.set(NONE);
						if (tasks.isEmpty() || !state.compareAndSet(NONE, RUNNING)) {
							executingThread.compareAndSet(current, null);
							return;
						}
					}
				}
			}
		}
		
		@Override
		public boolean inEventLoop(Thread thread) {
			return executingThread.get() == thread;
		}
		
		@Override
		public boolean isShuttingDown() {
			return executor.isShutdown();
		}
		
		@Override
		public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
			return executor.shutdownGracefully(quietPeriod, timeout, unit); 
		}
		
		@Override
		public Future<?> terminationFuture() {
			return executor.terminationFuture();
		}
		
		@Override
		public void shutdown() {
			executor.shutdown();
		}
		
		@Override
		public boolean isShutdown() {
			return executor.isShutdown();
		}
		
		@Override
		public boolean isTerminated() {
			return executor.isTerminated();
		}
		
		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return executor.awaitTermination(timeout, unit);
		}
		
		@Override
		public void execute(Runnable command) {
			if (!tasks.offer(command)) {
				throw new RejectedExecutionException();
			}
			if (state.compareAndSet(NONE, SUBMITTED)) {
				executor.execute(this);
			}
		}
	}
}
