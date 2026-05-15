package netty.channel;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import netty.common.util.concurrent.EventExecutorGroup;

public interface ChannelPipeline 
	extends ChannelInboundInvoker, ChannelOutboundInvoker, Iterable<Entry<String, ChannelHandler>>{
	
	ChannelPipeline addFirst(String name, ChannelHandler handler);
	
	ChannelPipeline addFirst(EventExecutorGroup group, String name, ChannelHandler handler);
	
	ChannelPipeline addLast(String name, ChannelHandler handler);
	
	ChannelPipeline addLast(EventExecutorGroup group, String name, ChannelHandler handler);
	
	ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler);
	
	ChannelPipeline addBefore(EventExecutorGroup group, String baseName, String name, ChannelHandler handler);
	
	ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler);
	
	ChannelPipeline addAfter(EventExecutorGroup group, String baseName, String name, ChannelHandler handler);
	
	ChannelPipeline addFirst(ChannelHandler... handlers);
	
	ChannelPipeline addFirst(EventExecutorGroup group, ChannelHandler... handlers);
	
	ChannelPipeline addLast(ChannelHandler... handlers);
	
	ChannelPipeline addLast(EventExecutorGroup group, ChannelHandler... handlers);
	
	ChannelPipeline remove(ChannelHandler handler);
	
	ChannelHandler remove(String name);
	
	<T extends ChannelHandler> T remove(Class<T> handlerType);
	
	ChannelHandler removeFirst();
	
	ChannelHandler removeLast();
	
	ChannelPipeline replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler);
	
	ChannelHandler replace(String oldName, String newName, ChannelHandler handler);
	
	<T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName,
			ChannelHandler newHandler);
	
	ChannelHandler first();
	
	ChannelHandlerContext firstContext();
	
	ChannelHandler last();
	
	ChannelHandlerContext lastContext();
	
	ChannelHandler get(String name);
	
	<T extends ChannelHandler> T get(Class<T> handlerType);
	
	ChannelHandlerContext context(ChannelHandler handler);
	
	ChannelHandlerContext context(String name);
	
	ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType);
	
	Channel channel();
	
	List<String> names();
	
	Map<String, ChannelHandler> toMap();
	
	@Override
	ChannelPipeline fireChannelRegistered();
	
	@Override
    ChannelPipeline fireChannelUnregistered();

    @Override
    ChannelPipeline fireChannelActive();

    @Override
    ChannelPipeline fireChannelInactive();

    @Override
    ChannelPipeline fireExceptionCaught(Throwable cause);

    @Override
    ChannelPipeline fireUserEventTriggered(Object event);

    @Override
    ChannelPipeline fireChannelRead(Object msg);

    @Override
    ChannelPipeline fireChannelReadComplete();

    @Override
    ChannelPipeline fireChannelWritabilityChanged();

    @Override
    ChannelPipeline flush();
    
    @Override
    default ChannelPromise newPromise() {
    	return new DefaultChannelPromise(channel());
    }
    
    @Override
    default ChannelProgressivePromise newProgressivePromise() {
    	return new DefaultChannelProgressivePromise(channel());
    }
    
    @Override
    default ChannelFuture newFailedFuture(Throwable cause) {
    	return new FailedChannelFuture(channel(), null, cause);
    }
}
