package netty.channel;

import netty.common.util.concurrent.EventExecutorGroup;

public interface EventLoopGroup extends EventExecutorGroup {
	
	@Override
	EventLoop next();
	
	ChannelFuture register(Channel channel);
	
	ChannelFuture register(ChannelPromise promise);
	
	ChannelFuture register(Channel channel, ChannelPromise promise);
}
