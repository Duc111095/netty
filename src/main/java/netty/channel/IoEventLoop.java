package netty.channel;

import netty.common.util.concurrent.Future;

public interface IoEventLoop extends EventLoop, IoEventLoopGroup {
	@Override
	default IoEventLoop next() {
		return this;
	}
	
	Future<IoRegistration> register(IoHandle handle);
	
	@Override
	boolean isCompatible(Class<? extends IoHandle> handleType);
	
	@Override
	boolean isIoType(Class<? extends IoHandler> handlerType);
}
