package netty.channel.nio;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import netty.channel.IoEvent;
import netty.channel.IoHandle;
import netty.channel.IoRegistration;
import netty.common.util.internal.ObjectUtil;

public abstract class NioSelectableChannelIoHandle<S extends SelectableChannel> implements IoHandle, NioIoHandle{
	private final S channel;
	
	public NioSelectableChannelIoHandle(S channel) {
		this.channel = ObjectUtil.checkNotNull(channel, "channel");
	}
	
	@Override
	public void handle(IoRegistration registration, IoEvent ioEvent) {
		SelectionKey key = registration.attachment();
		NioSelectableChannelIoHandle.this.handle(channel, key);
	}
	
	@Override
	public void close() throws Exception {
		channel.close();
	}
	
	@Override
	public SelectableChannel selectableChannel() {
		return channel;
	}
	
	protected abstract void handle(S channel, SelectionKey key);
	
	protected void deregister(S channel) {
		
	}
}
