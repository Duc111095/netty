package netty.channel;

import netty.common.util.concurrent.AbstractEventExecutorGroup;

public abstract class AbstractEventLoopGroup extends AbstractEventExecutorGroup implements EventLoopGroup {

	@Override
	public abstract EventLoop next();
}
