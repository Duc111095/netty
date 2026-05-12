package netty.channel;

import java.util.concurrent.Future;

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
	ChannelFuture syncInterruptibly();
	
	@Override
	ChannelFuture await() throws InterupptedException;
	
	@Override
	ChannelFuture awaitUninterruptibly();
	
	boolean isVoid();
}

