package netty.channel;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.GenericFutureListener;
import netty.common.util.concurrent.Promise;

public interface ChannelPromise extends ChannelFuture, Promise<Void> {
	
	@Override
	Channel channel();
	
	@Override
	ChannelPromise setSuccess(Void result);
	
	ChannelPromise setSuccess();
	
	boolean trySuccess();
	
	@Override
	ChannelPromise setFailure(Throwable cause);
	
	@Override
	ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener);
	
	@SuppressWarnings("unchecked")
	@Override
	ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);
	
	@Override
	ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener);

	@SuppressWarnings("unchecked")
	@Override
	ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);
	
	@Override
	ChannelPromise sync() throws InterruptedException;
	
	@Override
	ChannelPromise syncUninterruptibly();
	
	@Override
	ChannelPromise await() throws InterruptedException;
	
	@Override
	ChannelPromise awaitUninterruptibly();
	
	ChannelPromise unvoid();
}
