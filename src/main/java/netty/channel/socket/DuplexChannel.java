package netty.channel.socket;

import netty.channel.Channel;
import netty.channel.ChannelFuture;
import netty.channel.ChannelPromise;

public interface DuplexChannel extends Channel{
	boolean isInputShutdown();
	
	ChannelFuture shutdownInput();
	
	ChannelFuture shutdownInput(ChannelPromise promise);
	
	boolean isOutputShutdown();
	
	ChannelFuture shutdownOutput();
	
	ChannelFuture shutdownOutput(ChannelPromise promise);
	
	boolean isShutdown();
	
	ChannelFuture shutdown();
	
	ChannelFuture shutdown(ChannelPromise promise);
}
