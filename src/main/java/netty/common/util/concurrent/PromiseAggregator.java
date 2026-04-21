package netty.common.util.concurrent;

import java.util.LinkedHashSet;
import java.util.Set;

import netty.common.util.internal.ObjectUtil;

public class PromiseAggregator<V, F extends Future<V>> implements GenericFutureListener<F>{
	
	private final Promise<?> aggregatePromise;
	private final boolean failPending;
	private Set<Promise<V>> pendingPromises;
	
	public PromiseAggregator(Promise<Void> aggregatePromise, boolean failPending) {
		this.aggregatePromise = ObjectUtil.checkNotNull(aggregatePromise, "aggregatePromise");
		this.failPending = failPending;
	}
	
	public PromiseAggregator(Promise<Void> aggregatePromise) {
		this(aggregatePromise, true);
	}
	
	@SafeVarargs
	public final PromiseAggregator<V, F> add(Promise<V>... promises) {
		ObjectUtil.checkNotNull(promises, "promises");
		if (promises.length == 0) {
			return this;
		}
		synchronized (this) {
			if (pendingPromises == null) {
				int size;
				if (promises.length > 1) {
					size = promises.length;
				} else {
					size = 2;
				}
				pendingPromises = new LinkedHashSet<Promise<V>>(size);
			}
			for (Promise<V> p : promises) {
				if (p == null) {
					continue;
				}
				pendingPromises.add(p);
				p.addListener(this);
			}
		}
		return this;
	}
	
	@Override
	public synchronized void operationComplete(F future) throws Exception {
		if (pendingPromises == null) {
			aggregatePromise.setSuccess(null);
		} else {
			pendingPromises.remove(future);
			if (!future.isSuccess()) {
				Throwable cause = future.cause();
				aggregatePromise.setFailure(cause);
				if (failPending) {
					for (Promise<V> pendingFuture : pendingPromises) {
						pendingFuture.setFailure(cause);
					}
				}
			} else {
				if (pendingPromises.isEmpty()) {
					aggregatePromise.setSuccess(null);
				}
			}
		}
	}
}
