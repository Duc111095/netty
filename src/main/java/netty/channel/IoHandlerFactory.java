package netty.channel;

import netty.common.util.concurrent.ThreadAwareExecutor;

public interface IoHandlerFactory {
	IoHandler newHandler(ThreadAwareExecutor ioExecutor);

	default boolean isChangingThreadSupported() {
		return false;
	}
}
