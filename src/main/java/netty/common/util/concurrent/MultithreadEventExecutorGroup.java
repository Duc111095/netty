package netty.common.util.concurrent;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netty.common.util.concurrent.AutoScalingEventExecutorChooserFactory.AutoScalingUtilizationMetric;
import netty.common.util.concurrent.EventExecutorChooserFactory.ObservableEventExecutorChooser;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public abstract class MultithreadEventExecutorGroup extends AbstractEventExecutorGroup {
	
	private final EventExecutor[] children;
	private final Set<EventExecutor> readonlyChildren;
	private final AtomicInteger terminatedChildren = new AtomicInteger();
	private final Promise<?> terminationFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
	private final EventExecutorChooserFactory.EventExecutorChooser chooser;
	
	protected MultithreadEventExecutorGroup(int nThreads, ThreadFactory threadFactory, Object... args) {
		this(nThreads, threadFactory == null ? null : new ThreadPerTaskExecutor(threadFactory), args);
	}
	
	protected MultithreadEventExecutorGroup(int nThreads, Executor executor, Object... args) {
		this(nThreads, executor, DefaultEventExecutorChooserFactory.INSTANCE, args);
	}
	
	protected MultithreadEventExecutorGroup(int nThreads, Executor executor,
			EventExecutorChooserFactory chooserFactory, Object... args) {
		checkPositive(nThreads, "nThreads");
		
		if (executor == null) {
			executor = new ThreadPerTaskExecutor(newDefaultThreadFactory());
		}
		
		children = new EventExecutor[nThreads];
		
		for (int i = 0; i < nThreads; i++) {
			boolean success = false;
			try {
				children[i] = newChild(executor, args);
				success = true;
			} catch (Exception e) {
				throw new IllegalStateException("failed to create a child event loop", e);
			} finally {
				if (!success) {
					for (int j = 0; j < i; j++) {
						children[j].shutdownGracefully();
					}
					
					for (int j = 0; j < i; j++) {
						EventExecutor e = children[j];
						try {
							while (!e.isTerminated()) {
								e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
							}
						} catch (InterruptedException interrupted) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}
		}
		
		chooser = chooserFactory.newChooser(children);
		
		final FutureListener<Object> terminationListener = future -> {
			if (terminatedChildren.incrementAndGet() == children.length) {
				terminationFuture.setSuccess(null);
			}
		};
		
		for (EventExecutor e : children) {
			e.terminationFuture().addListener(terminationListener);
		}
		
		Set<EventExecutor> childrenSet = new LinkedHashSet<EventExecutor>(children.length);
		Collections.addAll(childrenSet, children);
		readonlyChildren = Collections.unmodifiableSet(childrenSet);
	} 
	
	protected ThreadFactory newDefaultThreadFactory() {
		return new DefaultThreadFactory(getClass());
	}
	
	@Override
	public EventExecutor next() {
		return chooser.next();
	}
	
	@Override
	public Iterator<EventExecutor> iterator() {
		return readonlyChildren.iterator();
	}
	
	public final int executorCount() {
		return children.length;
	}
	
	public int activeExecutorCount() {
		if (chooser instanceof ObservableEventExecutorChooser) {
			return ((ObservableEventExecutorChooser) chooser).activeExecutorCount();
		} 
		return executorCount();
	}
	
	public List<AutoScalingUtilizationMetric> executorUtilizations() {
		if (chooser instanceof ObservableEventExecutorChooser) {
			return ((ObservableEventExecutorChooser) chooser).executorUtilizations();
		}
		return Collections.emptyList();
	}
	
	protected abstract EventExecutor newChild(Executor executor, Object... args) throws Exception;

	@Override
	public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		for (EventExecutor l : children) {
			l.shutdownGracefully(quietPeriod, timeout, unit);
		}
		return terminationFuture();
	}
	
	@Override
	public Future<?> terminationFuture() {
		return terminationFuture;
	}
	
	@Override
	public void shutdown() {
		for (EventExecutor l : children) {
			l.shutdown();
		}
	}
	
	@Override
	public boolean isShuttingDown() {
		for (EventExecutor l : children) {
			if (!l.isShuttingDown()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public boolean isShutdown() {
		for (EventExecutor l : children) {
			if (!l.isShutdown()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public boolean isTerminated() {
		for (EventExecutor l : children) {
			if (!l.isTerminated()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) 
			throws InterruptedException {
		long deadline = System.nanoTime() + unit.toNanos(timeout);
		loop: for (EventExecutor l : children) {
			for (;;) {
				long timeLeft = deadline - System.nanoTime();
				if (timeLeft <= 0) {
					break loop;
				}
				if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
					break;
				}
			}
		}
		return isTerminated();
	}
}
