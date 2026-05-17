package netty.channel.nio;

import netty.channel.IoEvent;

public interface NioIoEvent extends IoEvent {
	
	NioIoOps ops();
}
