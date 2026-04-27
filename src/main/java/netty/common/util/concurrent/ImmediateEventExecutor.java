package netty.common.util.concurrent;

import java.util.ArrayDeque;
import java.util.Queue;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

public final class ImmediateEventExecutor extends AbstractEventExecutor {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ImmediateEventExecutor.class);
	public static final ImmediateEventExecutor INSTANCE = new ImmediateEventExecutor();
	
	private static final FastThreadLocal<Queue<Runnable>> DELAYED_RUNNABLES = new FastThreadLocal<Queue<Runnable>>() {
		@Override
		protected Queue<Runnable> initialValue() throws Exception {
			return new ArrayDeque<Runnable>();
		}
	};
	
	private static final FastThreadLocal<Boolean> RUNNING = new FastThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() throws Exception {
			return false;
		}
	};
	
	private final Future<?> terminationFuture = new FailedFuture<Object>(
			GlobalEventExecutor.INSTANCE, new UnsupportedOperationException());
	
	private ImmediateEventExecutor() {}
	
	@Override
	public boolean inEventLoop() {
		return true;
	}
	
	@Override
	public boolean inEventLoop(Thread thread) {
		return true;
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
	public void shutdown() {}
	
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
	public void execute(Runnable command) {
		ObjectUtil.checkNotNull(command, "command");
		if (!RUNNING.get()) {
			RUNNING.set(true);
			try {
				command.run();
			} catch (Throwable cause) {
				logger.info("Throwable caught while executing Runnable {}", command, cause);
			} finally {
				Queue<Runnable> delayedRunnables = DELAYED_RUNNABLES.get();
				Runnable runnable;
				while ((runnable = delayedRunnables.poll()) != null) {
					try {
						runnable.run();
					} catch (Throwable cause) {
						logger.info("Throwable caught while executing Runnable {}", runnable, cause);
					}
				}
				RUNNING.set(false);
			}
		} else {
			DELAYED_RUNNABLES.get().add(command);
		}
	}
	
	@Override
	public <V> Promise<V> newPromise() {
		return new ImmediatePromise<V>(this);
	}
	
	@Override
	public <V> ProgressivePromise<V> newProgressivePromise() {
		return new ImmediateProgressivePromise<V>(this);
	}

	static class ImmediatePromise<V> extends DefaultPromise<V> {
		ImmediatePromise(EventExecutor executor) {
			super(executor);
		}
		
		@Override
		protected void checkDeadLock() {
			
		}
	}
	
	static class ImmediateProgressivePromise<V> extends DefaultProgressivePromise<V> {
		ImmediateProgressivePromise(EventExecutor executor) {
			super(executor);
		}
		
		@Override
		protected void checkDeadLock() {
			
		}
	}
}
