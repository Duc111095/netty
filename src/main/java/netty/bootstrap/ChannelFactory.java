package netty.bootstrap;

import netty.channel.Channel;

public interface ChannelFactory<T extends Channel> {
	
	T newChannel();
}
