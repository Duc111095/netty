package netty.channel;

import java.util.concurrent.TimeUnit;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.AbstractFuture;
import netty.common.util.concurrent.GenericFutureListener;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.UnstableApi;

@UnstableApi
public final class VoidChannelPromise extends AbstractFuture<Void> implements ChannelPromise{

	private final Channel channel;
	private final ChannelFutureListener fireExceptionListener;
	
	public VoidChannelPromise(final Channel channel, boolean fireException) {
		ObjectUtil.checkNotNull(channel, "channel");
		this.channel = channel;
		if (fireException) {
			fireExceptionListener = new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					Throwable cause = future.cause();
					if (cause != null) {
						fireException0(cause);
					}
				}
			};
		} else {
			fireExceptionListener = null;
		}
	}
	
	@Override
	public VoidChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener ) {
		fail();
		return this;
	}
	
	@Override
    public VoidChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        fail();
        return this;
    }

    @Override
    public VoidChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        // NOOP
        return this;
    }

    @Override
    public VoidChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        // NOOP
        return this;
    }
    
    @Override
    public VoidChannelPromise await() throws InterruptedException {
    	if (Thread.interrupted()) {
    		throw new InterruptedException();
    	}
    	return this;
    }
    
    @Override
    public boolean await(long timeout, TimeUnit unit) {
    	fail();
    	return false;
    }
    
    @Override
    public boolean await(long timeoutMillis) {
    	fail();
    	return false;
    }
    
    @Override
    public VoidChannelPromise awaitUninterruptibly() {
    	fail();
    	return this;
    }
    
    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        fail();
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        fail();
        return false;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public boolean setUncancellable() {
        return true;
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public VoidChannelPromise sync() {
        fail();
        return this;
    }

    @Override
    public VoidChannelPromise syncUninterruptibly() {
        fail();
        return this;
    }

    @Override
    public VoidChannelPromise setFailure(Throwable cause) {
        fireException0(cause);
        return this;
    }

    @Override
    public VoidChannelPromise setSuccess() {
        return this;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        fireException0(cause);
        return false;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean trySuccess() {
        return false;
    }

    private static void fail() {
        throw new IllegalStateException("void future");
    }

    @Override
    public VoidChannelPromise setSuccess(Void result) {
        return this;
    }

    @Override
    public boolean trySuccess(Void result) {
        return false;
    }

    @Override
    public Void getNow() {
        return null;
    }

    @Override
    public ChannelPromise unvoid() {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        if (fireExceptionListener != null) {
            promise.addListener(fireExceptionListener);
        }
        return promise;
    }

    @Override
    public boolean isVoid() {
        return true;
    }
    
    private void fireException0(Throwable cause) {
    	if (fireExceptionListener != null && channel.isRegistered()) {
    		channel.pipeline().fireExceptionCaught(cause);
    	}
    }
}
