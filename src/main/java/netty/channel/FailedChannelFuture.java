package netty.channel;

import netty.common.util.concurrent.EventExecutor;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;

public class FailedChannelFuture extends CompleteChannelFuture {
	private final Throwable cause;
	
	FailedChannelFuture(Channel channel, EventExecutor executor, Throwable cause) {
		super(channel, executor);
		this.cause = ObjectUtil.checkNotNull(cause, "cause");
	}
	
	@Override
	public Throwable cause() {
		return cause;
	}
	
	@Override
	public boolean isSuccess() {
		return false;
	}
	
	@Override
	public ChannelFuture sync() {
		PlatformDependent.throwException(cause);
		return this;
	}
	
	@Override
	public ChannelFuture syncUninterruptibly() {
		PlatformDependent.throwException(cause);
		return this;
	}
}
