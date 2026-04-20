package netty.common.util.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.Async.Execute;

import netty.common.util.internal.UnstableApi;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public abstract class AbstractEventExecutor extends AbstractExecutorService implements EventExecutor {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractEventExecutor.class);
	
	static final long DEFAULT_SHUTDOWN_QUIET_PERIOD = 2;
	static final long DEFAULT_SHUTDOWN_TIMEOUT = 15;
	
	private final EventExecutorGroup parent;
	private final Collection<EventExecutor> selfCollection = Collections.<EventExecutor>singleton(this);
	
	protected AbstractEventExecutor() {
		this(null);
	}
	
	protected AbstractEventExecutor(EventExecutorGroup parent) {
		this.parent = parent;
	}
	
	@Override
	public EventExecutorGroup parent() {
		return parent;
	}
	
	@Override
	public EventExecutor next() {
		return this;
	}
	
	@Override
	public Iterator<EventExecutor> iterator() {
		return selfCollection.iterator();
	}
	
	@Override
	public Future<?> shutdownGracefully() {
		return shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
	}
	
	@Override
	public abstract void shutdown();
	
	@Override
	public List<Runnable> shutdownNow() {
		shutdown();
		return Collections.emptyList();
	}
	
	@Override
	public Future<?> submit(Runnable task) {
		return (Future<?>) super.submit(task);
	}
	
	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return (Future<T>) super.submit(task, result);
	}
	
	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return (Future<T>) super.submit(task);
	}
	
	@Override
	protected final <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return new PromiseTask<T>(this, runnable, value);
	}
	
	@Override
	protected final <T> RunnableFuture<T> newTaskFor(Callable task) {
		return new PromiseTask<T>(this, task);
	}
	
	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, 
			TimeUnit unit) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		throw new UnsupportedOperationException();
	}
	
	protected static void safeExecute(Runnable task) {
		try {
			runTask(task);
		} catch (Throwable t) {
			logger.warn("A task raised an exception. Task: {}", task, t);
		}
	}
	
	protected static void runTask(@Execute Runnable task) {
		task.run();
	}
	
	@UnstableApi
	public void lazyExecute(Runnable task) { 
		execute(task);
	}
	
	public interface LazyRunnable extends Runnable { }
}
