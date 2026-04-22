package netty.common.util.concurrent;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkNotNullWithIAE;

import netty.common.util.internal.PromiseNotificationUtil;

import static netty.common.util.internal.ObjectUtil.checkNotNull;


public class PromiseNotifier<V, F extends Future<V>> implements GenericFutureListener<F> {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(PromiseNotifier.class);
	private final Promise<? super V>[] promises;
	private final boolean logNotifyFailure;
	
	@SuppressWarnings("unchecked")
	public PromiseNotifier(Promise<? super V>... promises) {
		this(true, promises);
	}
	
	public PromiseNotifier(boolean logNotifyFailure, Promise<? super V>... promises) {
		checkNotNull(promises, "promises");
		for (Promise<? super V> promise : promises) {
			checkNotNullWithIAE(promise, "promise");
		}
		this.promises = promises.clone();
		this.logNotifyFailure = logNotifyFailure;
	}
	
	public static <V, F extends Future<V>> F cascade(final F future, final Promise<? super V> promise) {
		return cascade(true, future, promise);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <V, F extends Future<V>> F cascade(boolean logNotifyFailure, final F future,
			final Promise<? super V> promise) {
		promise.addListener((FutureListener) f -> {
			if (f.isCancelled()) {
				future.cancel(false);
			}
		});
		future.addListener(new PromiseNotifier(logNotifyFailure, promise) {
			@Override
			public void operationComplete(Future f) throws Exception {
				if (promise.isCancelled() && f.isCancelled()) {
					return;
				}
				super.operationComplete(future);
			}
		});
		return future;
	}
	
	@Override
	public void operationComplete(F future) throws Exception {
		InternalLogger internalLogger = logNotifyFailure ? logger : null;
		if (future.isSuccess()) {
			V result = future.get();
			for (Promise<? super V> p : promises) {
				PromiseNotificationUtil.trySuccess(p, result, internalLogger);
			}
		} else if (future.isCancelled()) {
			for (Promise<? super V> p : promises) {
				PromiseNotificationUtil.tryCancel(p, internalLogger);
			}
		} else {
			Throwable cause = future.cause();
			for (Promise<? super V> p : promises) {
				PromiseNotificationUtil.tryFailure(p, cause, internalLogger);
			}
		}
		
	}
}
