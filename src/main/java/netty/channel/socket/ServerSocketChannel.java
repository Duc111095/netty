package netty.channel.socket;

import java.net.InetSocketAddress;

import netty.channel.ServerChannel;

public interface ServerSocketChannel extends ServerChannel {
	@Override
	ServerSocketChannelConfig config();
	@Override
	InetSocketAddress localAddress();
	@Override
	InetSocketAddress remoteAddress();
}
