package netty.channel;

import netty.common.util.concurrent.CompleteFuture;
import netty.common.util.concurrent.EventExecutor;
import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.GenericFutureListener;
import netty.common.util.internal.ObjectUtil;

abstract class CompleteChannelFuture extends CompleteFuture<Void> implements ChannelFuture {
	
	private final Channel channel;
	
	protected CompleteChannelFuture(Channel channel, EventExecutor executor) {
		super(executor);
		this.channel = ObjectUtil.checkNotNull(channel, "channel");
	}
	
	@Override
	protected EventExecutor executor() {
		EventExecutor e = super.executor();
		if (e == null) {
			return channel().eventLoop();
		} else {
			return e;
		}
	}
	
	@Override
	public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
		super.addListener(listener);
		return this;
	}
	
    @SuppressWarnings("unchecked")
	@Override
    public ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        super.removeListener(listener);
        return this;
    }

    @SuppressWarnings("unchecked")
	@Override
    public ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        super.removeListeners(listeners);
        return this;
    }

    @Override
    public ChannelFuture syncUninterruptibly() {
        return this;
    }

    @Override
    public ChannelFuture sync() throws InterruptedException {
        return this;
    }

    @Override
    public ChannelFuture await() throws InterruptedException {
        return this;
    }

    @Override
    public ChannelFuture awaitUninterruptibly() {
        return this;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Void getNow() {
        return null;
    }

    @Override
    public boolean isVoid() {
        return false;
    }
}
