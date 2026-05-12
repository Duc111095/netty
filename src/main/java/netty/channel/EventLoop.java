package netty.channel;

import netty.common.util.concurrent.OrderedEventExecutor;

public interface EventLoop extends OrderedEventExecutor, EventLoopGroup {

	@Override
	EventLoopGroup parent();
}
