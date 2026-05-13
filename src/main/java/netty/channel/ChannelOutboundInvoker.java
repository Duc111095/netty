package netty.channel;

import java.net.SocketAddress;

public interface ChannelOutboundInvoker {
	
	default ChannelFuture bind(SocketAddress localAddress) {
		return bind(localAddress, newPromise());
	}
	
	default ChannelFuture connect(SocketAddress remoteAddress) {
		return connect(remoteAddress, newPromise());
	}
	
	default ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
		return connect(remoteAddress, localAddress, newPromise());
	}
	
	default ChannelFuture disconnect() {
		return disconnect(newPromise());
	}
	
	default ChannelFuture close() {
		return close(newPromise());
	}
	
	default ChannelFuture deregister() {
		return deregister(newPromise());
	}
	
	ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise);
	
	ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise);
	
	ChannelFuture connect(SocketAddress remoteAddress, SocketAddress local, ChannelPromise promise);
	
	ChannelFuture disconnect(ChannelPromise promise);
	
	ChannelFuture close(ChannelPromise promise);
	
	ChannelFuture deregister(ChannelPromise promise);
	
	ChannelOutboundInvoker read();
	
	default ChannelFuture write(Object msg) {
		return write(msg, newPromise());
	}
	
	ChannelFuture write(Object msg, ChannelPromise promise);

	ChannelOutboundInvoker flush();
	
	ChannelFuture writeAndFlush(Object msg, ChannelPromise promise);
	
	default ChannelFuture writeAndFlush(Object msg) {
		return writeAndFlush(msg, newPromise());
	}
	
	ChannelPromise newPromise();
	
	ChannelProgressivePromise newProgressivePromise();
	
	ChannelFuture newSucceededFuture();
	
	ChannelFuture newFailedFuture(Throwable cause);
	
	ChannelPromise voidPromise();
	
}
