package netty.channel;

import netty.channel.ChannelHandlerMask.Skip;

public class ChannelInboundHandlerAdapter extends ChannelHandlerAdapter implements ChannelInboundHandler {
	
	@Skip
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelRegistered();
	}
	
	@Skip
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelUnregistered();
	}
	
	@Skip
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();
	}
	
	@Skip
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelInactive();
	}
	
	@Skip
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ctx.fireChannelRead(msg);
	}
	
	@Skip
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelReadComplete();
	}

	@Skip
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		ctx.fireUserEventTriggered(evt);
	}

	@Skip
	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelWritabilityChanged();
	}
	
	@Skip
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		throws Exception {
		ctx.fireExceptionCaught(cause);
	}
}
