package netty.channel;

import netty.common.util.internal.UnstableApi;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.ObjectUtil.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.GenericFutureListener;
import netty.common.util.internal.PromiseNotificationUtil;

@UnstableApi
public final class DelegatingChannelPromiseNotifier implements ChannelPromise, ChannelFutureListener {
	private static final InternalLogger logger = 
			InternalLoggerFactory.getInstance(DelegatingChannelPromiseNotifier.class);
	private final ChannelPromise delegate;
	private final boolean logNotifyFailure;
	
	public DelegatingChannelPromiseNotifier(ChannelPromise delegate) {
		this(delegate, !(delegate instanceof VoidChannelPromise));
	}
	
	public DelegatingChannelPromiseNotifier(ChannelPromise delegate, boolean logNotifyFailure) {
		this.delegate = checkNotNull(delegate, "delegate");
		this.logNotifyFailure = logNotifyFailure;
	}
	
	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		InternalLogger internalLogger = logNotifyFailure ? logger : null;
		if (future.isSuccess()) {
			Void result = future.get();
			PromiseNotificationUtil.trySuccess(delegate, result, internalLogger);
		} else if (future.isCancelled()) {
			PromiseNotificationUtil.tryCancel(delegate, internalLogger);
		} else {
			Throwable cause = future.cause();
			PromiseNotificationUtil.tryFailure(delegate, cause, internalLogger);
		}
	}
	
	@Override
	public Channel channel() {
		return delegate.channel();
	}
	
	@Override
	public ChannelPromise setSuccess(Void result) {
		delegate.setSuccess(result);
		return this;
	}
	
	@Override
	public ChannelPromise setSuccess() {
		delegate.setSuccess();
		return this;
	}
	
	@Override
    public boolean trySuccess() {
        return delegate.trySuccess();
    }

    @Override
    public boolean trySuccess(Void result) {
        return delegate.trySuccess(result);
    }

    @Override
    public ChannelPromise setFailure(Throwable cause) {
        delegate.setFailure(cause);
        return this;
    }

    @Override
    public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        delegate.addListener(listener);
        return this;
    }

    @Override
    public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        delegate.addListeners(listeners);
        return this;
    }

    @Override
    public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        delegate.removeListener(listener);
        return this;
    }

    @Override
    public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        delegate.removeListeners(listeners);
        return this;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        return delegate.tryFailure(cause);
    }

    @Override
    public boolean setUncancellable() {
        return delegate.setUncancellable();
    }

    @Override
    public ChannelPromise await() throws InterruptedException {
        delegate.await();
        return this;
    }

    @Override
    public ChannelPromise awaitUninterruptibly() {
        delegate.awaitUninterruptibly();
        return this;
    }

    @Override
    public boolean isVoid() {
        return delegate.isVoid();
    }

    @Override
    public ChannelPromise unvoid() {
        return isVoid() ? new DelegatingChannelPromiseNotifier(delegate.unvoid()) : this;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.await(timeout, unit);
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return delegate.await(timeoutMillis);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        return delegate.awaitUninterruptibly(timeout, unit);
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        return delegate.awaitUninterruptibly(timeoutMillis);
    }

    @Override
    public Void getNow() {
        return delegate.getNow();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    @Override
    public ChannelPromise sync() throws InterruptedException {
        delegate.sync();
        return this;
    }

    @Override
    public ChannelPromise syncUninterruptibly() {
        delegate.syncUninterruptibly();
        return this;
    }

    @Override
    public boolean isSuccess() {
        return delegate.isSuccess();
    }

    @Override
    public boolean isCancellable() {
        return delegate.isCancellable();
    }

    @Override
    public Throwable cause() {
        return delegate.cause();
    }
}
