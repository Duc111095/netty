package netty.channel;

import netty.common.util.ReferenceCountUtil;
import netty.common.util.internal.TypeParameterMatcher;

public abstract class SimpleUserEventChannelHandler<I> extends ChannelInboundHandlerAdapter {
	private final TypeParameterMatcher matcher;
	private final boolean autoRelease;
	
	protected SimpleUserEventChannelHandler() {
		this(true);
	}
	
	protected SimpleUserEventChannelHandler(boolean autoRelease) {
		matcher = TypeParameterMatcher.find(this, SimpleUserEventChannelHandler.class, "I");
		this.autoRelease = autoRelease;
	}
	
	protected SimpleUserEventChannelHandler(Class<? extends I> eventType) {
		this(eventType, true);
	}
	
	protected SimpleUserEventChannelHandler(Class<? extends I> eventType, boolean autoRelease) {
		matcher = TypeParameterMatcher.get(eventType);
		this.autoRelease = autoRelease;
	}
	
	protected boolean acceptEvent(Object evt) throws Exception {
		return matcher.match(evt);
	}
	
	@Override
	public final void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		boolean release = true;
		try {
			if (acceptEvent(evt)) {
				@SuppressWarnings("unchecked")
				I ievt = (I) evt;
				eventReceived(ctx, ievt);
			} else {
				release = false;
				ctx.fireUserEventTriggered(evt);
			}
		} finally {
			if (autoRelease && release) {
				ReferenceCountUtil.release(evt);
			}
		}
	}
	
	protected abstract void eventReceived(ChannelHandlerContext ctx, I evt) throws Exception;
}
