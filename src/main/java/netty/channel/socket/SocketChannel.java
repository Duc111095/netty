package netty.channel.socket;

import java.net.InetSocketAddress;

public interface SocketChannel extends DuplexChannel {
	@Override
	ServerSocketChannel parent();
	
	@Override
	SocketChannelConfig config();
	@Override
	InetSocketAddress localAddress();
	@Override
	InetSocketAddress remoteAddress();
}
