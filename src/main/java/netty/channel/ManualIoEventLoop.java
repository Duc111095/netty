package netty.channel;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import netty.common.util.concurrent.AbstractScheduledEventExecutor;
import netty.common.util.concurrent.DefaultPromise;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.GlobalEventExecutor;
import netty.common.util.concurrent.Promise;
import netty.common.util.concurrent.Ticker;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.ThreadExecutorMap;

public class ManualIoEventLoop extends AbstractScheduledEventExecutor implements IoEventLoop {
	private static final Runnable WAKEUP_TASK = () -> {
		
	};
	private static final int ST_STARTED = 0;
	private static final int ST_SHUTTING_DOWN = 1;
	private static final int ST_SHUTDOWN = 2;
	private static final int ST_TERMINATED = 3;
	
	private final AtomicInteger state;
	private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);
	private final Queue<Runnable> taskQueue = PlatformDependent.newMpscQueue();
	private final IoHandlerContext nonBlockingContext = new IoHandlerContext() {
		@Override
		public boolean canBlock() {
			assert inEventLoop();
			return false;
		}
		
		@Override
		public long delayNanos(long currentTimeNanos) {
			assert inEventLoop();
			return 0;
		}
		
		@Override
		public long deadlineNanos() {
			assert inEventLoop();
			return -1;
		}
	};
	private final BlockingIoHandlerContext blockingContext = new BlockingIoHandlerContext();
	private final IoEventLoopGroup parent;
	private final AtomicReference<Thread> owningThread;
	private final IoHandler handler;
	private final Ticker ticker;
	
	private volatile long gracefulShutdownQuietPeriod;
	private volatile long gracefulShutdownTimeout;
	private long gracefulShutdownStartTime;
	private long lastExecutionTime;
	private boolean initialized;
	
	protected boolean canBlock() {
		return true;
	}
	
	public ManualIoEventLoop(Thread owningThread, IoHandlerFactory factory) {
		this(null, owningThread, factory);
	}
	
	public ManualIoEventLoop(IoEventLoopGroup parent, Thread owningThread, IoHandlerFactory factory) {
		this(parent, owningThread, factory, Ticker.systemTicker());
	}
	
	public ManualIoEventLoop(IoEventLoopGroup parent, Thread owningThread, IoHandlerFactory factory, Ticker ticker) {
		this.parent = parent;
		this.owningThread = new AtomicReference<>(owningThread);
		this.handler = factory.newHandler(this);
		this.ticker = Objects.requireNonNull(ticker, "ticker");
		state = new AtomicInteger(ST_STARTED);
	}
	
	@Override
	public final Ticker ticker() {
		return ticker;
	}
	
	public final int runNonBlockingTasks(long timeoutNanos) {
		return runAllTasks(timeoutNanos, true);
	}
	
	private int runAllTasks(long timeoutNanos, boolean setCurrentExecutor) {
		assert inEventLoop();
		final Queue<Runnable> taskQueue = this.taskQueue;
		boolean alwaysTrue = fetchFromScheduledTaskQueue(taskQueue);
		assert alwaysTrue;
		Runnable task = taskQueue.poll();
		if (task == null) {
			return 0;
		}
		EventExecutor old = setCurrentExecutor ? ThreadExecutorMap.setCurrentExecutor(this) : null;
		try {
			final long deadline = timeoutNanos > 0 ? getCurrentTimeNanos() + timeoutNanos : 0;
			int runTasks = 0;
			long lastExecutionTime;
			final Ticker ticker = this.ticker;
			for (;;) {
				safeExecute(task);
				
				runTasks++;
				
				if (timeoutNanos > 0) {
					lastExecutionTime = ticker.nanoTime();
					if ((lastExecutionTime - deadline) >= 0) {
						break;
					}
				}
				
				task = taskQueue.poll();
				if (task == null) {
					lastExecutionTime = ticker.nanoTime();
					break;
				}
			}
			this.lastExecutionTime = lastExecutionTime;
			return runTasks;
		} finally {
			if (setCurrentExecutor) {
				ThreadExecutorMap.setCurrentExecutor(old);
			}
		}
	}
	
	private int run(IoHandlerContext context, long runAllTasksTimeoutNanos) {
		if (!initialized) {
			if (owningThread.get() == null) {
				throw new IllegalStateException("Owning thread not set");
			}
			initialized = true;
			handler.initialize();
		}
		EventExecutor old = ThreadExecutorMap.setCurrentExecutor(this);
		try {
			if (isShuttingDown()) {
				if (terminationFuture.isDone()) {
					return 0;
				}
				return runAllTasksBeforeDestroy();
			}
			final int ioTasks = handler.run(context);
			if (runAllTasksTimeoutNanos < 0) {
				return ioTasks;
			}
			assert runAllTasksTimeoutNanos >= 0;
			return ioTasks + runAllTasks(runAllTasksTimeoutNanos, false);
		} finally {
			ThreadExecutorMap.setCurrentExecutor(old);
		}
	}
	
	private int runAllTasksBeforeDestroy() {
		int run = runAllTasks(-1, false);
		handler.prepareToDestroy();
		if (confirmShutdown()) {
			try {
				handler.destroy();
				for (;;) {
					int r = runAllTasks(-1, false);
					run += r;
					if (r == 0) {
						break;
					}
				}
			} finally {
				state.set(ST_TERMINATED);
				terminationFuture.setSuccess(null);
			}
		}
		return run;
	}
	
	public final int runNow(long runAllTasksTimeoutNanos) {
		checkCurrentThread();
		return run(nonBlockingContext, runAllTasksTimeoutNanos);
	}
	
	public final int runNow() {
		checkCurrentThread();
		return run(nonBlockingContext, 0);
	}
	
	private final int run(long waitNanos, long runAllTasksTimeoutNanos) {
		checkCurrentThread();
		
		final IoHandlerContext context;
		if (waitNanos < 0) {
			context = nonBlockingContext;
		} else {
			context = blockingContext;
			blockingContext.maxBlockingNanos = waitNanos == 0 ? Long.MAX_VALUE : waitNanos;
		}
		return run(context, runAllTasksTimeoutNanos);
	}
	
	public final int run(long waitNanos) {
		return run(waitNanos, 0);
	}
	
	private void checkCurrentThread() {
		if (!inEventLoop(Thread.currentThread())) {
			throw new IllegalStateException();
		}
	}
	
	public final void wakeup() {
		if (isShuttingDown()) {
			return;
		}
		handler.wakeup();
	}
	
	@Override
	public final ManualIoEventLoop next() {
		return this;
	}
	
	@Override
	public final IoEventLoopGroup parent() {
		return parent;
	}
	
	@Override
	public final ChannelFuture register(Channel channel) {
		return register(new DefaultChannelPromise(channel, this));
	}
	
	@Override
	public final ChannelFuture register(final ChannelPromise promise) {
		ObjectUtil.checkNotNull(promise, "promise");
		promise.channel().unsafe().register(this, promise);
		return promise;
	}
	
	@Override
	public final Future<IoRegistration> register(final IoHandle handle) {
		Promise<IoRegistration> promise = newPromise();
		if (inEventLoop() ) {
			registerForIo0(handle, promise);
		} else {
			execute(() -> registerForIo0(handle, promise));
		}
		
		return promise;
	}
	
	private void registerForIo0(final IoHandle handle, Promise<IoRegistration> promise) {
		assert inEventLoop();
		final IoRegistration registration;
		try {
			registration = handler.register(handle);
		} catch (Exception e) {
			promise.setFailure(e);
			return;
		}
		promise.setSuccess(registration);
	}
	
	@Override
	public final ChannelFuture register(final Channel channel, final ChannelPromise promise) {
		ObjectUtil.checkNotNull(promise, "promise");
		ObjectUtil.checkNotNull(channel, "channel");
		channel.unsafe().register(this, promise);
		return promise;
	}
	
	@Override
	public final boolean isCompatible(Class<? extends IoHandle> handleType) {
		return handler.isCompatible(handleType);
	}
	
	@Override
	public final boolean isIoType(Class<? extends IoHandler> handlerType) {
		return handler.getClass().equals(handlerType);
	}
	
	@Override
	public final boolean inEventLoop(Thread thread) {
		return this.owningThread.get() == thread;
	}
	
	public final void setOwningThread(Thread owningThread) {
		Objects.requireNonNull(owningThread, "owningThread");
		if (!this.owningThread.compareAndSet(null, owningThread)) {
			throw new IllegalStateException("owningThread already set");
		}
	}
	
	private void shutdown0(long quietPeriod, long timeout, int shutdownState) {
		boolean inEventLoop = inEventLoop();
		boolean wakeup;
		int oldState;
		for (;;) {
			if (isShuttingDown()) {
				return;
			}
			int newState;
			wakeup = true;
			oldState = state.get();
			if (inEventLoop) {
				newState = shutdownState;
			} else if (oldState == ST_STARTED) {
				newState = shutdownState;
			} else {
				newState = oldState;
				wakeup = false;
			}
			
			if (state.compareAndSet(oldState, newState)) {
				break;
			}
		}
		if (quietPeriod != -1) {
			gracefulShutdownQuietPeriod = quietPeriod;
		}
		if (timeout != -1) {
			gracefulShutdownTimeout = timeout;
		}
		if (wakeup) {
			taskQueue.offer(WAKEUP_TASK);
			handler.wakeup();
		}
	}
	
	@Override
	public final Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
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
	public final void shutdown() {
		shutdown0(-1, -1, ST_SHUTDOWN);
	}
	
	@Override
	public final Future<?> terminationFuture() {
		return terminationFuture;
	}
	
	@Override
	public final boolean isShuttingDown() {
		return state.get() >= ST_SHUTTING_DOWN;
	}
	
	@Override
	public final boolean isShutdown() {
		return state.get() >= ST_SHUTDOWN;
	}
	
	@Override
	public final boolean isTerminated() {
		return state.get() == ST_TERMINATED;
	}
	
	@Override
	public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return terminationFuture.await(timeout, unit);
	}
	
	@Override
	public final void execute(Runnable command) {
		Objects.requireNonNull(command, "command");
		boolean inEventLoop = inEventLoop();
		if (inEventLoop) {
			if (isShutdown()) {
				throw new RejectedExecutionException("event executor terminated");
			}
		}
		taskQueue.add(command);
		if (!inEventLoop) {
			if (isShutdown()) {
				boolean reject = false;
				try {
					if (taskQueue.remove(command)) {
						reject = true;
					}
				} catch (UnsupportedOperationException e) {
					
				}
				if (reject) {
					throw new RejectedExecutionException("event executor terminated");
				}
			}
			handler.wakeup();
		}
	}
	
	private boolean hasTasks() {
		return !taskQueue.isEmpty();
	}
	
	private boolean confirmShutdown() {
		if (!isShuttingDown()) {
			return false;
		}
		
		if (!inEventLoop()) {
			throw new IllegalStateException("must be invoked from an event loop");
		}
		
		cancelScheduledTasks();
		if (gracefulShutdownStartTime == 0) {
			gracefulShutdownStartTime = ticker.nanoTime();
		}
		
		if (runAllTasks(-1, false) > 0) {
			if (isShutdown()) {
				return true;
			}
			if (gracefulShutdownQuietPeriod == 0) {
				return true;
			}
			return false;
		}
		
		final long nanoTime = ticker.nanoTime();
		if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
			return true;
		}
		
		if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				
			}
			return false;
		}
		return true;
	}
	
	@Override
	public final <T> T invokeAny(Collection<? extends Callable<T>> tasks) 
			throws InterruptedException, ExecutionException {
		throwIfInEventLoop("invokeAny");
		return super.invokeAny(tasks);
	}
	
	@Override
	public final <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
			throws InterruptedException, ExecutionException, TimeoutException {
		throwIfInEventLoop("invokeAny");
		return super.invokeAny(tasks, timeout, unit);
	}
	
	@Override
	public final <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) 
			throws InterruptedException {
		throwIfInEventLoop("invokeAll");
		return super.invokeAll(tasks);
	}
	
	@Override
	public final <T> List<java.util.concurrent.Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		throwIfInEventLoop("invokeAll");
		return super.invokeAll(tasks, timeout, unit);
	}
	
	private void throwIfInEventLoop(String method) {
		if (inEventLoop()) {
			throw new RejectedExecutionException(
					"Calling " + method + " from within the EventLoop  is not allowed as it would deadlock");
		}
	}
	
	private class BlockingIoHandlerContext implements IoHandlerContext {
		long maxBlockingNanos = Long.MAX_VALUE;
		
		@Override
		public boolean canBlock() {
			assert inEventLoop();
			return !hasTasks() && !hasScheduledTasks() && ManualIoEventLoop.this.canBlock();
		}
		
		@Override
		public long delayNanos(long currentTimeNanos) {
			assert inEventLoop();
			return Math.min(maxBlockingNanos, ManualIoEventLoop.this.delayNanos(currentTimeNanos, maxBlockingNanos));
		}
		
		@Override
		public long deadlineNanos() {
			assert inEventLoop();
			long next = nextScheduledTaskDeadlineNanos();
			if (maxBlockingNanos == Long.MAX_VALUE) {
				return next;
			}
			long now = ticker.nanoTime();
			if (next == -1 || next - now > maxBlockingNanos) {
				return now + maxBlockingNanos;
			}
			return next;
		}
	}
}
