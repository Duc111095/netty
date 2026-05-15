package netty.channel;

import netty.common.util.concurrent.GenericFutureListener;
import netty.common.util.concurrent.ProgressiveFuture;
import netty.common.util.concurrent.Future;

public interface ChannelProgressiveFuture extends ChannelFuture, ProgressiveFuture<Void>{

	@Override
    ChannelProgressiveFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener);

    @Override
    ChannelProgressiveFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    @Override
    ChannelProgressiveFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener);

    @Override
    ChannelProgressiveFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    @Override
    ChannelProgressiveFuture sync() throws InterruptedException;

    @Override
    ChannelProgressiveFuture syncUninterruptibly();

    @Override
    ChannelProgressiveFuture await() throws InterruptedException;

    @Override
    ChannelProgressiveFuture awaitUninterruptibly();
}
