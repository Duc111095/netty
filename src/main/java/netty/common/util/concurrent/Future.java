package netty.common.util.concurrent;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("ClassNameSameAsAncestorName")
public interface Future<V> extends java.util.concurrent.Future<V> {
	
	boolean isSuccess();
	
	boolean isCancellable();
	
	Throwable cause();
	
	Future<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

	Future<V> addListeners(@SuppressWarnings("unchecked") GenericFutureListener<? extends Future<? super V>>... listeners);
	
	Future<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);
	
	Future<V> removeListeners(@SuppressWarnings("unchecked") GenericFutureListener<? extends Future<? super V>>... listeners);

	Future<V> sync() throws InterruptedException;
	
	Future<V> syncUninterruptibly();
	
	Future<V> await() throws InterruptedException;
	
	Future<V> awaitUninterruptibly();
	
	boolean await(long timeout, TimeUnit unit) throws InterruptedException;
	
	boolean await(long timeoutMillis) throws InterruptedException;
	
	boolean awaitUninterruptibly(long timeout, TimeUnit unit);
	
	boolean awaitUninterruptibly(long timeoutMillis);
	
	V getNow();
	
	@Override
	boolean cancel(boolean mayInterruptIfRunning);
}
