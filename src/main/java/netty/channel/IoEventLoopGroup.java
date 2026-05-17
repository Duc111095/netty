package netty.channel;

import java.util.concurrent.Future;

public interface IoEventLoopGroup extends EventLoopGroup {
	
	@Override
	IoEventLoop next();
	
	@Override
	default ChannelFuture register(Channel channel) {
		return next().register(channel);
	}
	
	@Override
	default ChannelFuture register(ChannelPromise promise) {
		return next().register(promise);
	}
	
	default Future<IoRegistration> register(IoHandle handle) {
		return next().register(handle);
	}
	
	default boolean isCompatible(Class<? extends IoHandle> handleType) {
		return next().isCompatible(handleType);
	}
	
	default boolean isIoType(Class<? extends IoHandler> handlerType) {
		return next().isIoType(handlerType);
	}
}
