package netty.channel;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.Promise;
import netty.common.util.concurrent.RejectedExecutionHandler;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.SystemPropertyUtil;

public class SingleThreadIoEventLoop extends SingleThreadEventLoop implements IoEventLoop {
	
	private static final long DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS = TimeUnit.MILLISECONDS.toNanos(Math.max(1,
			SystemPropertyUtil.getInt("io.netty.eventLoop.maxTaskProcessingQuantumMs", 1000)));
	
	private final long maxTaskProcessingQuantumNs;
	private final IoHandlerContext context = new IoHandlerContext() {
		@Override
		public boolean canBlock() {
			assert inEventLoop();
			return !hasTasks() && !hasScheduledTasks();
		}
		
		@Override
		public long delayNanos(long currentTimeNanos) {
			assert inEventLoop();
			return SingleThreadIoEventLoop.this.delayNanos(currentTimeNanos);
		}
		
		@Override
		public long deadlineNanos() {
			assert inEventLoop();
			return SingleThreadIoEventLoop.this.deadlineNanos();
		}
		
		@Override
		public void reportActiveIoTime(long activeNanos) {
			SingleThreadIoEventLoop.this.reportActiveIoTime(activeNanos);
		}
		
		@Override
		public boolean shouldReportActiveIoTime() {
			return isSuspensionSupported();
		}
	};
	
	private final IoHandler ioHandler;
	private final AtomicInteger numRegistrations = new AtomicInteger();

	public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
			IoHandlerFactory ioHandlerFactory) {
		super(parent, threadFactory, false,
				ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported());
		this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
		this.ioHandler = ioHandlerFactory.newHandler(this);
	}
	
	public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor, IoHandlerFactory ioHandlerFactory) {
		super(parent, executor, false,
				ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported());
		this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
		this.ioHandler = ioHandlerFactory.newHandler(this);
	}
	
	public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
			IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
			RejectedExecutionHandler rejectedExecutionHandler, long maxTaskProcessingQuantumMs) {
		super(parent, threadFactory, false,
				ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),
				maxPendingTasks, rejectedExecutionHandler);
		this.maxTaskProcessingQuantumNs =
				ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
						DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
						TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
		this.ioHandler = ioHandlerFactory.newHandler(this);
	}
	
	public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
								   IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
								   RejectedExecutionHandler rejectedExecutionHandler,
								   long maxTaskProcessingQuantumMs) {
		super(parent, executor, false,
				ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),
				maxPendingTasks, rejectedExecutionHandler);
		this.maxTaskProcessingQuantumNs = 
				ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
						DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
						TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
		this.ioHandler = ioHandlerFactory.newHandler(this);
	}
	
	protected SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
									  IoHandlerFactory ioHandlerFactory, Queue<Runnable> taskQueue,
									  Queue<Runnable> tailTaskQueue,
									  RejectedExecutionHandler rejectedExecutionHandler) {
		super(parent, executor, false,
				ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),
				taskQueue, tailTaskQueue, rejectedExecutionHandler);
		this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
		this.ioHandler = ioHandlerFactory.newHandler(this);
	}
	
	@Override
	protected void run() {
		assert inEventLoop();
		ioHandler.initialize();
		do {
			runIo();
			if (isShuttingDown()) {
				ioHandler.prepareToDestroy();
			}
			runAllTasks(maxTaskProcessingQuantumNs);
		} while (!confirmShutdown() && !canSuspend());
	}
	
	protected final IoHandler ioHandler() {
		return ioHandler;
	}
	
	@Override
	protected boolean canSuspend(int state) {
		return super.canSuspend(state) && numRegistrations.get() == 0;
	}
	
	protected int runIo() {
		assert inEventLoop();
		return ioHandler.run(context);
	}
	
	@Override
	public IoEventLoop next() {
		return this;
	}
	
	@Override
	public final Future<IoRegistration> register(final IoHandle handle) {
		Promise<IoRegistration> promise = newPromise();
		if (inEventLoop()) {
			registerForIo0(handle, promise);
		} else {
			execute(() -> registerForIo0(handle, promise));
		}
		
		return promise;
	}
	
	@Override
	protected int getNumOfRegisteredChannels() {
		return numRegistrations.get();
	}
	
	private void registerForIo0(final IoHandle handle, Promise<IoRegistration> promise) {
		assert inEventLoop();
		final IoRegistration registration;
		try {
			registration = ioHandler.register(handle);
		} catch (Exception e) {
			promise.setFailure(e);
			return;
		}
		numRegistrations.incrementAndGet();
		promise.setSuccess(new IoRegistrationWrapper(registration));
	}
	
	@Override
	protected final void wakeup(boolean inEventLoop) {
		ioHandler.wakeup();
	}
	
	@Override
	protected final void cleanup() {
		assert inEventLoop();
		ioHandler.destroy();
	}
	
	@Override
	public boolean isCompatible(Class<? extends IoHandle> handleType) {
		return ioHandler.isCompatible(handleType);
	}
	
	@Override
	public boolean isIoType(Class<? extends IoHandler> handlerType) {
		return ioHandler.getClass().equals(handlerType);
	}
	
	protected static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
		return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue() :
			PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
	}
	
	private final class IoRegistrationWrapper implements IoRegistration {
		private final IoRegistration registration;
		IoRegistrationWrapper(IoRegistration registration) {
			this.registration = registration;
		}
		
		@Override
		public <T> T attachment() {
			return  registration.attachment();
		}
		
		@Override
		public long submit(IoOps ops) {
			return registration.submit(ops);
		}
		
		@Override
		public boolean isValid() {
			return registration.isValid();
		}
		
		@Override
		public boolean cancel() {
			if (registration.cancel()) {
				numRegistrations.decrementAndGet();
				return true;
			}
			return false;
		}
	}
} 
