package netty.channel;

public interface ChannelInboundInvoker {
	
	ChannelInboundInvoker fireChannelRegistered();
	
	ChannelInboundInvoker fireChannelUnregistered();
	
	ChannelInboundInvoker fireChannelActive();
	
	ChannelInboundInvoker fireChannelInactive();
	
	ChannelInboundInvoker fireExceptionCaught(Throwable cause);
	
	ChannelInboundInvoker fireUserEventTriggered(Object event);
	
	ChannelInboundInvoker fireChannelRead(Object msg);
	
	ChannelInboundInvoker fireChannelReadComplete();
	
	ChannelInboundInvoker fireChannelWritabilityChanged();
}
