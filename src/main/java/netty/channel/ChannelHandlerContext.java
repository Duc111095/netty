package netty.channel;

import netty.buffer.ByteBufAllocator;
import netty.common.util.Attribute;
import netty.common.util.AttributeKey;
import netty.common.util.AttributeMap;
import netty.common.util.concurrent.EventExecutor;

public interface ChannelHandlerContext extends AttributeMap, ChannelInboundInvoker, ChannelOutboundInvoker {
	
	Channel channel();
	
	EventExecutor executor();
	
	String name();
	
	ChannelHandler handler();
	
	boolean isRemoved();
	
	@Override
	ChannelHandlerContext fireChannelRegistered();
	
	@Override
    ChannelHandlerContext fireChannelUnregistered();

    @Override
    ChannelHandlerContext fireChannelActive();

    @Override
    ChannelHandlerContext fireChannelInactive();

    @Override
    ChannelHandlerContext fireExceptionCaught(Throwable cause);

    @Override
    ChannelHandlerContext fireUserEventTriggered(Object evt);

    @Override
    ChannelHandlerContext fireChannelRead(Object msg);

    @Override
    ChannelHandlerContext fireChannelReadComplete();

    @Override
    ChannelHandlerContext fireChannelWritabilityChanged();

    @Override
    ChannelHandlerContext read();

    @Override
    ChannelHandlerContext flush();
    
    ChannelPipeline pipeline();
    
    ByteBufAllocator alloc();
    
    @Override
    <T> Attribute<T> attr(AttributeKey<T> key);
    
    @Override
    <T> boolean hasAttr(AttributeKey<T> key);
}
