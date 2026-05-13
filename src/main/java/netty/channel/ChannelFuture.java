package netty.channel;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.GenericFutureListener;

public interface ChannelFuture extends Future<Void>{	
	Channel channel();
	
	@Override
	ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener);
	
	@Override
	ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);
	
	@Override
	ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener);
	
	@Override
	ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

	@Override
	ChannelFuture sync() throws InterruptedException;
	
	@Override
	ChannelFuture syncUninterruptibly();
	
	@Override
	ChannelFuture await() throws InterruptedException;
	
	@Override
	ChannelFuture awaitUninterruptibly();
	
	boolean isVoid();
}

