package netty.common.util.concurrent;

import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import netty.common.util.internal.InternalThreadLocalMap;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.StringUtil;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.ThrowableUtil;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DefaultPromise<V> extends AbstractFuture<V> implements Promise<V> {
	
	public static final String PROPERTY_MAX_LISTENER_STACK_DEPTH = "io.netty.defaultPromise.maxListenerStackDepth";
	
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultPromise.class);
	private static final InternalLogger rejectedExecutionLogger =
			InternalLoggerFactory.getInstance(DefaultPromise.class.getName() + ".rejectedExecution");
	private static final int MAX_LISTENER_STACK_DEPTH = Math.min(0,
			SystemPropertyUtil.getInt(PROPERTY_MAX_LISTENER_STACK_DEPTH, 8));
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<DefaultPromise, Object> RESULT_UPDATER = 
			AtomicReferenceFieldUpdater.newUpdater(DefaultPromise.class, Object.class, "result");
	private static final Object SUCCESS = new Object();
	private static final Object UNCANCELLABLE = new Object();
	private static final CauseHolder CANCELLATION_CAUSE_HOLDER = new CauseHolder(
			StacklessCancellationException.newInstance(DefaultPromise.class, "cancel(...)"));
	private static final StackTraceElement[] CANCELLATION_STACK = CANCELLATION_CAUSE_HOLDER.cause.getStackTrace();
	
	private volatile Object result;
	private final EventExecutor executor;
	
	private GenericFutureListener<? extends Future<?>> listener;
	private DefaultFutureListeners listeners;
	
	private short waiters;
	
	private boolean notifyingListeners;
	
	public DefaultPromise(EventExecutor executor) {
		this.executor = checkNotNull(executor, "executor");
	}
	
	protected DefaultPromise() {
		executor = null;
	}
	
	@Override
	public Promise<V> setSuccess(V result) {
		if (setSuccess0(result)) {
			return this;
		}
		throw new IllegalStateException("complete already: " + this);
	}
	
	@Override
	public boolean trySuccess(V result) {
		return setSuccess0(result);
	}
	
	@Override
	public Promise<V> setFailure(Throwable cause) {
		if (setFailure0(cause)) {
			return this;
		}
		throw new IllegalStateException("complete already: " + this, cause);
	}
	
	@Override
	public boolean tryFailure(Throwable cause) {
		return setFailure0(cause);
	}
	
	@Override
	public boolean setUncancellable() {
		if (RESULT_UPDATER.compareAndSet(this, null, UNCANCELLABLE)) {
			return true;
		}
		Object result = this.result;
		return !isDone0(result) || !isCancelled0(result);
	}
	
	@Override
	public boolean isSuccess() {
		Object result = this.result;
		return result != null && result != UNCANCELLABLE && !(result instanceof CauseHolder);
	}
	
	@Override
	public boolean isCancellable() {
		return result == null;
	}
	
	private static final class LeanCancellationException extends CancellationException {
		
		private static final long serialVersionUID = 6151904822098546586L;

		@Override
		public Throwable fillInStackTrace() {
			setStackTrace(CANCELLATION_STACK);
			return this;
		}
		
		@Override
		public String toString() {
			return CancellationException.class.getName();
		}
	}
	
	@Override
	public Throwable cause() {
		return cause0(result);
	}
	
	private Throwable cause0(Object result) {
		if (!(result instanceof CauseHolder)) {
			return null;
		}
		if (result == CANCELLATION_CAUSE_HOLDER) {
			CancellationException ce = new LeanCancellationException();
			if (RESULT_UPDATER.compareAndSet(this, CANCELLATION_CAUSE_HOLDER, new CauseHolder(ce))) {
				return ce;
			}
			result = this.result;
		}
		return ((CauseHolder) result).cause;
	}
	
	@Override
	public Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener) {
		checkNotNull(listener, "listeners");
		synchronized (this) {
			addListener0(listener);
		}
		
		if (isDone()) {
			notifyListeners();
		}
		return this;
	}
	
	@Override
	public Promise<V> addListeners(@SuppressWarnings("unchecked") GenericFutureListener<? extends Future<? super V>>... listeners) {
		checkNotNull(listeners, "listeners");
		
		synchronized (this) {
			for (GenericFutureListener<? extends Future<? super V>> listener : listeners) {
				if (listener == null) {
					break;
				}
				addListener0(listener);
			}
		}
		
		if (isDone()) {
			notifyListeners();
		}
		return this;
	}
	
	@Override
	public Promise<V> removeListener(final GenericFutureListener<? extends Future<? super V>> listener) {
		checkNotNull(listener, "listener");
		synchronized (this) {
			removeListener0(listener);
		}
		return this;
	}
	
	@Override
	public Promise<V> removeListeners(@SuppressWarnings("unchecked") final GenericFutureListener<? extends Future<? super V>>... listeners) {
		checkNotNull(listeners, "listeners");
		
		synchronized (this ) {
			for (GenericFutureListener<? extends Future<? super V>> listener : listeners) {
				if (listener == null) {
					break;
				}
				
				removeListener0(listener);
			}
		}
		return this;
	}
	
	@Override
	public Promise<V> await() throws InterruptedException {
		if (isDone()) {
			return this;
		}
		
		if (Thread.interrupted()) {
			throw new InterruptedException(toString());
		}
		
		checkDeadLock();
		
		synchronized (this ) {
			while (!isDone()) {
				incWaiters();
				try {
					wait();
				} finally {
					decWaiters();
				}
			}
		}
		return this;
	}
	
	@Override
	public Promise<V> awaitUninterruptibly() {
		if (isDone()) {
			return this;
		}
		
		checkDeadLock();
		boolean interrupted = false;
		synchronized (this ) {
			while (!isDone()) {
				incWaiters();
				try {
					wait();
				} catch (InterruptedException e) {
					interrupted = true;
				} finally {
					decWaiters();
				}
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
		return this;
	}
	
	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return await0(unit.toNanos(timeout), true);
	}
	
	@Override
	public boolean await(long timeoutMillis) throws InterruptedException {
		return await0(MILLISECONDS.toNanos(timeoutMillis), true);
	}
	
	@Override
	public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
		try {
			return await0(unit.toNanos(timeout), false);
		} catch (InterruptedException e) {
			throw new InternalError();
		}
	}
	
	@Override
	public boolean awaitUninterruptibly(long timeoutMillis) {
		try {
			return await0(MILLISECONDS.toNanos(timeoutMillis), false);
		} catch (InterruptedException e) {
			throw new InternalError();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V getNow() {
		Object result = this.result;
		if (result instanceof CauseHolder || result == SUCCESS || result == UNCANCELLABLE) {
			return null;
		}
		return (V) result;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V get() throws InterruptedException, ExecutionException {
		Object result = this.result;
		if (!isDone0(result)) {
			await();
			result = this.result;
		}
		if (result == SUCCESS || result == UNCANCELLABLE) {
			return null;
		}
		Throwable cause = cause0(result);
		if (cause == null) {
			return (V) result;
		}
		if (cause instanceof CancellationException) {
			throw (CancellationException) cause;
		}
		throw new ExecutionException(cause);
 	}
	
	@SuppressWarnings("unchecked")
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		Object result = this.result;
		if (!isDone0(result)) {
			if (!await(timeout, unit)) {
				throw new TimeoutException("timeout after " + timeout + " " + unit.name().toLowerCase(Locale.ENGLISH));
			}
			result = this.result;
		}
		if (result == SUCCESS || result == UNCANCELLABLE) {
			return null;
		}
		Throwable cause = cause0(result);
		if (cause == null) {
			return (V) result;
		}
		if (cause instanceof CancellationException) {
			throw (CancellationException) cause;
		}
		throw new ExecutionException(cause);
	}
	
	@Override
	public boolean cancel(boolean maybeInterruptIfRunning) {
		if (RESULT_UPDATER.compareAndSet(this, null, CANCELLATION_CAUSE_HOLDER)) {
			if (checkNotifyWaiters()) {
				notifyListeners();
			}
			return true;
		}
		return false;
	}
	
	@Override
	public boolean isCancelled() {
		return isCancelled0(result);
	}
	
	@Override
	public boolean isDone() {
		return isDone0(result);
	}
	
	@Override
	public Promise<V> sync() throws InterruptedException {
		await();
		rethrowIfFailed();
		return this;
	}
	
	@Override
	public Promise<V> syncUninterruptibly() {
		awaitUninterruptibly();
		rethrowIfFailed();
		return this;
	}
	
	@Override
	public String toString() {
		return toStringBuilder().toString();
	}
	
	protected StringBuilder toStringBuilder( ) {
		StringBuilder buf = new StringBuilder(64)
				.append(StringUtil.simpleClassName(this))
				.append('@')
				.append(Integer.toHexString(hashCode()));
		Object result = this.result;
		if (result == SUCCESS) {
			buf.append("(success)");
		} else if (result == UNCANCELLABLE) {
			buf.append("(uncancellable)");
		} else if (result instanceof CauseHolder) {
			buf.append("(failure: ")
				.append(((CauseHolder)result).cause)
				.append(')');
		} else if (result != null) {
			buf.append("(success: ")
				.append(result)
				.append(')');
		} else {
			buf.append("(incomplete)");
		}
		return buf;
	}
	
	protected EventExecutor executor() {
		return executor;
	}
	
	protected void checkDeadLock() {
		EventExecutor e = executor();
		if (e != null && e.inEventLoop()) {
			throw new BlockingOperationException(toString());
		}
	}
	
	protected static void notifyListener(
			EventExecutor eventExecutor, final Future<?> future, final GenericFutureListener<?> listener) {
		notifyListenerWithStackOverFlowProtection(
				checkNotNull(eventExecutor, "eventExecutor"),
				checkNotNull(future, "future"),
				checkNotNull(listener, "listener"));
	}
	
	private void notifyListeners() {
		EventExecutor executor = executor();
		if (executor.inEventLoop()) {
			final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
			final int stackDepth = threadLocals.futureListenerStackDepth();
			if (stackDepth < MAX_LISTENER_STACK_DEPTH) {
				threadLocals.setFutureListenerStackDepth(stackDepth + 1);
				try {
					notifyListenersNow();
				} finally {
					threadLocals.setFutureListenerStackDepth(stackDepth);
				}
				return;
			}
		}
		
		safeExecute(executor, new Runnable() {
			@Override
			public void run() {
				notifyListenersNow();
			}
		});
	}
	
	private static void notifyListenerWithStackOverFlowProtection(
			final EventExecutor executor,
			final Future<?> future,
			final GenericFutureListener<?> listener) {
		if (executor.inEventLoop()) {
			final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
			final int stackDepth = threadLocals.futureListenerStackDepth();
			if (stackDepth < MAX_LISTENER_STACK_DEPTH) {
				threadLocals.setFutureListenerStackDepth(stackDepth + 1);
				try {
					notifyListener0(future, listener);
				} finally {
					threadLocals.setFutureListenerStackDepth(stackDepth);
				}
				return;
			}
		}
		
		safeExecute(executor, new Runnable() {
			@Override
			public void run() {
				notifyListener0(future, listener);
			}
		});
	}
	
	@SuppressWarnings("rawtypes")
	private void notifyListenersNow() {
		GenericFutureListener listener;
		DefaultFutureListeners listeners;
		synchronized (this) {
			listener = this.listener;
			listeners = this.listeners;
			if (notifyingListeners || (listener == null && listeners == null)) {
				return;
			}
			notifyingListeners = true;
			if (listener != null) {
				this.listener = null;
			} else {
				this.listeners = null;
			}
		}
		for (;;) {
			if (listener != null) {
				notifyListener0(this, listener);
			} else {
				notifyListeners0(listeners);
			}
			synchronized (this) {
				if (this.listener == null && this.listeners == null) {
					notifyingListeners = false;
					return;
				}
				listener = this.listener;
				listeners = this.listeners;
				if (listener != null) {
					this.listener = null;
				} else {
					this.listeners = null;
				}
			}
		}
	}
	
	private void notifyListeners0(DefaultFutureListeners listeners) {
		GenericFutureListener<?>[] a = listeners.listeners();
		int size = listeners.size();
		for (int i = 0; i < size; i++) {
			notifyListener0(this, a[i]);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void notifyListener0(Future future, GenericFutureListener l) {
		try {
			l.operationComplete(future);
		} catch (Throwable t) {
			if (logger.isWarnEnabled()) {
				logger.warn("An exeception was thrown by " + l.getClass().getName() + ".operationComplete()");
			}
		}
	}
	
	private void addListener0(GenericFutureListener<? extends Future<? super V>> listener) {
		if (this.listener == null) {
			if (listeners == null) {
				this.listener = listener;
			} else {
				listeners.add(listener);
			}
		} else {
			assert listeners == null;
			listeners = new DefaultFutureListeners(this.listener, listener);
			this.listener = null;
		}
	}
	
	private void removeListener0(GenericFutureListener<? extends Future<? super V>> toRemove) {
		if (listener == toRemove) {
			listener = null;
		} else if (listeners != null) {
			listeners.remove(toRemove);
			if (listeners.size() == 0) {
				listeners = null;
			}
		}
	}
	
	private boolean setSuccess0(V result ) {
		return setValue0(result == null ? SUCCESS : result);
	}
	
	private boolean setFailure0(Throwable cause) {
		return setValue0(new CauseHolder(checkNotNull(cause, "cause")));
	}
	
	private boolean setValue0(Object objResult) {
		if (RESULT_UPDATER.compareAndSet(this, null, objResult) ||
			RESULT_UPDATER.compareAndSet(this, UNCANCELLABLE, objResult)) {
			if (checkNotifyWaiters()) {
				notifyListeners();
			}
			return true;
		}
		return false;
	}
	
	private synchronized boolean checkNotifyWaiters() {
		if (waiters > 0) {
			notifyAll();
		}
		return listener != null || listeners != null;
	}
	
	private void incWaiters() {
		if (waiters == Short.MAX_VALUE) {
			throw new IllegalStateException("too many waiters: " + this);
		}
		++waiters;
	}
	
	private void decWaiters() {
		--waiters;
	}
	
	private void rethrowIfFailed() {
		Throwable cause = cause();
		if (cause == null) {
			return;
		}
		if (!(cause instanceof CancellationException) && cause.getSuppressed().length == 0) {
			cause.addSuppressed(new CompletionException("Rethrowing promise failure cause", null));
		}
		PlatformDependent.throwException(cause);
	}
	
	private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
		if (isDone()) {
			return true;
		}
		if (timeoutNanos <= 0) {
			return isDone();
		}
		
		if (interruptable && Thread.interrupted()) {
			throw new InterruptedException(toString());
		}
		
		checkDeadLock();
		
		final long startTime = System.nanoTime();
		synchronized (this) {
			boolean interrupted = false;
			try {
				long waitTime = timeoutNanos;
				while (!isDone() && waitTime > 0) {
					incWaiters();
					try {
						wait(waitTime / 1000000, (int) (waitTime % 1000000));
					} catch (InterruptedException e) {
						if (interruptable) {
							throw e;
						} else {
							interrupted = true;
						}
					} finally {
						decWaiters();
					}
					if (isDone()) {
						return true;
					}
					waitTime = timeoutNanos - (System.nanoTime() - startTime);
				}
				return isDone();
			} finally {
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	void notifyProgressiveListeners(final long progress, final long total) {
		final Object listeners = progressiveListeners();
		if (listeners == null) {
			return;
		}
		
		final ProgressiveFuture<V> self = (ProgressiveFuture<V>) this;
		
		EventExecutor executor = executor();
		
		if (executor.inEventLoop()) {
			if (listeners instanceof GenericProgressiveFutureListener[]) {
				notifyProgressiveListeners0(
						self, (GenericProgressiveFutureListener<?>[]) listeners, progress, total);
			} else {
				notifyProgressiveListener0(
						self, (GenericProgressiveFutureListener<ProgressiveFuture<V>>) listeners, progress, total);
			}
		} else {
			if (listeners instanceof GenericProgressiveFutureListener[]) {
				final GenericProgressiveFutureListener<?>[] array =
						(GenericProgressiveFutureListener<?>[]) listeners;
				safeExecute(executor, new Runnable() {
					@Override
					public void run() {
						notifyProgressiveListeners0(self, array, progress, total);
					}
				});
			} else {
				final GenericProgressiveFutureListener<ProgressiveFuture<V>> l =
						(GenericProgressiveFutureListener<ProgressiveFuture<V>>) listeners;
				safeExecute(executor, new Runnable() {
					@Override
					public void run() {
						notifyProgressiveListener0(self, l, progress, total);
					}
				});
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private synchronized Object progressiveListeners() {
		final GenericFutureListener listener = this.listener;
		final DefaultFutureListeners listeners = this.listeners;
		if (listeners == null && listener == null) {
			return null;
		}
		if (listeners != null) {
			DefaultFutureListeners dfl = listeners;
			int progressiveSize = dfl.progressiveSize();
			switch (progressiveSize) {
			case 0:
				return null;
			case 1:
				for (GenericFutureListener<?> l : dfl.listeners()) {
					if (l instanceof GenericProgressiveFutureListener) {
						return l;
					}
				}
				return null;
			}
			
			GenericFutureListener<?>[] array = dfl.listeners();
			GenericProgressiveFutureListener<?>[] copy = new GenericProgressiveFutureListener[progressiveSize];
			for (int i = 0, j = 0; j < progressiveSize; i++) {
				GenericFutureListener<?> l = array[i];
				if (l instanceof GenericProgressiveFutureListener) {
					copy[j++] = (GenericProgressiveFutureListener<?>) l; 
				}
			}
			return copy;
		} else if (listener instanceof GenericProgressiveFutureListener) {
			return listener;
		} else {
			return null;
		}
	}
	
	private static void notifyProgressiveListeners0(
			ProgressiveFuture<?> future, GenericProgressiveFutureListener<?>[] listeners, long progress, long total) {
		for (GenericProgressiveFutureListener<?> l : listeners) {
			if (l == null) {
				break;
			}
			notifyProgressiveListener0(future, l, progress, total);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void notifyProgressiveListener0(
			ProgressiveFuture future, GenericProgressiveFutureListener l, long progress, long total) {
		try {
			l.operationProgressed(future, progress, total);
		} catch (Throwable t) {
			if (logger.isWarnEnabled()) {
				logger.warn("An exception was thrown by " + l.getClass().getName() + ".operationProgressed()");
			}
		}
	}
	
	private static boolean isCancelled0(Object result) {
		return result instanceof CauseHolder && ((CauseHolder) result).cause instanceof CancellationException;
	}
	
	private static boolean isDone0(Object result) {
		return result != null && result != UNCANCELLABLE;
	}
	
	private static final class CauseHolder {
		final Throwable cause;
		CauseHolder(Throwable cause) {
			this.cause = cause;
		}
	}
	
	private static void safeExecute(EventExecutor executor, Runnable task) {
		try {
			executor.execute(task);
		} catch (Throwable t) {
			rejectedExecutionLogger.error("Failed to submit a listener notification task. Event loop shutdown?", t);
		}
	}
	
	private static final class StacklessCancellationException extends CancellationException {

		private static final long serialVersionUID = -2291609578952455629L;
		
		private StacklessCancellationException() {}
		
		@Override
		public Throwable fillInStackTrace() {
			return this;
		}
		
		static StacklessCancellationException newInstance(Class<?> clazz, String method) {
			return ThrowableUtil.unknownStackTrace(new StacklessCancellationException(), clazz, method);
		}
	}
}
