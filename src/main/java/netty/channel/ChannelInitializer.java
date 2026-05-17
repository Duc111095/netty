package netty.channel;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import netty.channel.ChannelHandler.Sharable;

@Sharable
public abstract class ChannelInitializer<C extends Channel> extends ChannelInboundHandlerAdapter {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelInitializer.class);
	
	private static Set<ChannelHandlerContext> initMap = ConcurrentHashMap.newKeySet();
	
	protected abstract void initChannel(C ch) throws Exception;
	
	@Override
	public final void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		if (initChannel(ctx)) {
			ctx.pipeline().fireChannelRegistered();
			
			removeState(ctx);
		} else {
			ctx.fireChannelRegistered();
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (logger.isWarnEnabled()) {
			logger.warn("Failed to initialize a channel. Closing: " + ctx.channel(), cause);
		}
		ctx.close();
	}
	
	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		if (ctx.channel().isRegistered()) {
			if (initChannel(ctx)) {
				removeState(ctx);
			}
		}
	}
	
	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		initMap.remove(ctx);
	}
	
	@SuppressWarnings("unchecked")
	private boolean initChannel(ChannelHandlerContext ctx) throws Exception {
		if (initMap.add(ctx)) {
			try {
				initChannel((C) ctx.channel());
			} catch (Throwable cause) {
				exceptionCaught(ctx, cause);
			} finally {
				if (!ctx.isRemoved()) {
					ctx.pipeline().remove(this);
				}
			}
			return true;
		}
		return false;
	}
	
	private void removeState(final ChannelHandlerContext ctx) {
		if (ctx.isRemoved()) {
			initMap.remove(ctx);
		} else {
			ctx.executor().execute(new Runnable() {
				@Override
				public void run() {
					initMap.remove(ctx);
				}
			});
		}
	}
}
