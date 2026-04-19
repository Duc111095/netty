package netty.common.util.concurrent;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface EventExecutorGroup extends ScheduledExecutorService, Iterable<EventExecutor> {
	
	boolean isShuttingDown();
	
	Future<?> shutdownGracefully();
	
	Future<?> shutdownGracefully(long quitePeriod, long timeout, TimeUnit  unit);
	
	Future<?> terminationFuture();
	
	void shutdown();
	
	List<Runnable> shutdownNow();
	
	EventExecutor next();
	
	Iterator<EventExecutor> iterator();
	
	Future<?> submit(Runnable task);
	
	<T> Future<T> submit(Runnable task, T result);
	
	<T> Future<T> submit(Callable<T> task);
	
	default Ticker ticker() {
		return Ticker.systemTicker();
	}
	
	ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);
	
	<V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);
	
	ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);
	
	ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);
}
