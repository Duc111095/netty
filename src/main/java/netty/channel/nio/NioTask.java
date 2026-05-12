package netty.channel.nio;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

public interface NioTask<C extends SelectableChannel> {
	
	void channelReady(C ch, SelectionKey key) throws Exception;
	
	void channelUnregitered(C ch, Throwable cause) throws Exception;
}
