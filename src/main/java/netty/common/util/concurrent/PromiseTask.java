package netty.common.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

class PromiseTask<V> extends DefaultPromise<V> implements RunnableFuture<V>{
	
	private static final class RunnableAdapter<T> implements Callable<T> {
		final Runnable task;
		final T result;
		
		RunnableAdapter(Runnable task, T result) {
			this.task = task;
			this.result = result;
		}
		
		@Override
		public T call() {
			task.run();
			return result;
		}
		
		@Override
		public String toString() {
			return "Callable(task: " + task + ", result: " + result + ")";
		}
	}
	
	private static final Runnable COMPLETED = new SentinelRunnable("COMPLETED");
	private static final Runnable CANCELLED = new SentinelRunnable("CANCELLED");
	private static final Runnable FAILED = new SentinelRunnable("FAILED");
	
	private static final class SentinelRunnable implements Runnable {
		private final String name;
		
		SentinelRunnable(String name) {
			this.name = name;
		}
		
		@Override
		public void run() {}
		
		@Override
		public String toString() {
			return name;
		}
	}
	
	private Object task;
	
	PromiseTask(EventExecutor executor, Runnable runnable, V result) {
		super(executor);
		task = result == null ? runnable : new RunnableAdapter<V>(runnable, result);
	}
	
	PromiseTask(EventExecutor executor, Runnable runnable) {
		super(executor);
		task = runnable;
	}
	
	PromiseTask(EventExecutor executor, Callable<V> callable) {
		super(executor);
		task = callable;
	}
	
	@Override
	public final int hashCode() {
		return System.identityHashCode(this);
	}
	
	@Override
	public final boolean equals(Object obj) {
		return this == obj;
	}
	
	@SuppressWarnings("unchecked")
	V runTask() throws Throwable {
		final Object task = this.task;
		if (task instanceof Callable) {
			return ((Callable<V>) task).call();
		}
		((Runnable) task).run();
		return null;
	}
	
	@Override
	public void run() {
		try {
			if (setUncancellableInternal()) {
				V result = runTask();
				setSuccessInternal(result);
			}
		} catch (Throwable e) {
			setFailureInternal(e);
		}
	}
	
	private boolean clearTaskAfterCompletion(boolean done, Runnable result) {
		if (done) {
			task = result;
		}
		return done;
	}
	
	@Override
	public final Promise<V> setFailure(Throwable cause) {
		throw new IllegalStateException();
	}
	
	protected final Promise<V> setFailureInternal(Throwable cause) {
		super.setFailure(cause);
		clearTaskAfterCompletion(true, FAILED);
		return this;
	}
	
	@Override
	public final boolean tryFailure(Throwable cause) {
		return false;
	}
	
	protected final boolean tryFailureInternal(Throwable cause) {
		return clearTaskAfterCompletion(super.tryFailure(cause), FAILED);
	}
	
	@Override
	public final Promise<V> setSuccess(V result) {
		throw new IllegalStateException();
	}
	
	protected final Promise<V> setSuccessInternal(V result) {
		super.setSuccess(result);
		clearTaskAfterCompletion(true, COMPLETED);
		return this;
	}
	
	@Override
	public final boolean trySuccess(V next) {
		return false;
	}
	
	protected final boolean trySuccessInternal(V result) {
		return clearTaskAfterCompletion(super.trySuccess(result), COMPLETED);
	}
	
	@Override
	public final boolean setUncancellable() {
		throw new IllegalStateException();
	}
	
	protected final boolean setUncancellableInternal() {
		return super.setUncancellable();
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return clearTaskAfterCompletion(super.cancel(mayInterruptIfRunning), CANCELLED);
	}
	
	@Override
	protected StringBuilder toStringBuilder() {
		StringBuilder buf = super.toStringBuilder();
		buf.setCharAt(buf.length() - 1, ',');
		
		return buf.append(" task: ")
				.append(task)
				.append(')');
	}
}

