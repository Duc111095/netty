package netty.channel.nio;

import java.nio.channels.SelectableChannel;

import netty.channel.IoHandle;

public interface NioIoHandle extends IoHandle {
	SelectableChannel selectableChannel();
}
