package netty.channel;

import netty.common.util.concurrent.AbstractEventExecutor;

public abstract class AbstractEventLoop extends AbstractEventExecutor implements EventLoop {
	
	protected AbstractEventLoop() {}
	
	protected AbstractEventLoop(EventLoopGroup parent) {
		super(parent);
	}
	
	@Override
	public EventLoopGroup parent() {
		return (EventLoopGroup) super.parent();
	}
	
	@Override
	public EventLoop next() {
		return (EventLoop) super.next();
	}
}
