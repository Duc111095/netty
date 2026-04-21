package netty.common.util.concurrent;

import netty.common.util.internal.ObjectUtil;

public final class PromiseCombiner {
	private int expectedCount;
	private int doneCount;
	private Promise<Void> aggregatePromise;
	private Throwable cause;
	private final EventExecutor executor;
	private final GenericFutureListener<Future<?>> listener = new GenericFutureListener<Future<?>>() {
		@Override
		public void operationComplete(final Future<?> future) {
			if (executor.inEventLoop()) {
				operationComplete0(future);
			} else {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						operationComplete0(future);
					}
				});
			}
		}
		
		private void operationComplete0(Future<?> future) {
			assert executor.inEventLoop();
			++doneCount;
			if (!future.isSuccess() && cause == null) {
				cause = future.cause();
			}
			if (doneCount == expectedCount && aggregatePromise != null) {
				tryPromise();
			}
		}
	};
	
	public PromiseCombiner() {
		this(ImmediateEventExecutor.INSTANCE);
	}
	
	public PromiseCombiner(EventExecutor executor) {
		this.executor = ObjectUtil.checkNotNull(executor, "executor");
	}
	
	@SuppressWarnings("rawtypes")
	public void add(Promise promise) {
		add((Future) promise);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void add(Future future) {
		checkAddAllowed();
		checkInEventLoop();
		++expectedCount;
		future.addListener(listener);
	}
	
	@SuppressWarnings("rawtypes")
	public void addAll(Promise... promises) {
		addAll((Future[]) promises);
	}
	
	@SuppressWarnings("rawtypes")
	public void addAll(Future... futures) {
		for (Future f : futures) {
			this.add(f);
		}
	}
	
	public void finish(Promise<Void> aggregatePromise) {
		ObjectUtil.checkNotNull(aggregatePromise, "aggregatePromise");
		checkInEventLoop();
		if (this.aggregatePromise != null) {
			throw new IllegalStateException("Already finished");
		}
		this.aggregatePromise = aggregatePromise;
		if (doneCount == expectedCount) {
			tryPromise();
		}
	}
	
	private void checkInEventLoop() {
		if (!executor.inEventLoop()) {
			throw new IllegalStateException("Must be called from EventExecutor thread");
		}
	}
	
	private boolean tryPromise() {
		return cause == null ? aggregatePromise.trySuccess(null) : aggregatePromise.tryFailure(cause);
	}
	
	private void checkAddAllowed() {
		if (aggregatePromise != null) {
			throw new IllegalStateException("Adding promises is not allowed after finished adding");
		}
	}
}
