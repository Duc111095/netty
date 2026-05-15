package netty.channel.socket;

import netty.buffer.ByteBufAllocator;
import netty.channel.ChannelConfig;
import netty.channel.MessageSizeEstimator;
import netty.channel.RecvByteBufAllocator;
import netty.channel.WriteBufferWaterMark;

public interface DuplexChannelConfig extends ChannelConfig {
	boolean isAllowHalfClosure();
	
	DuplexChannelConfig setAllowHalfClosure(boolean allowHalfClosure);
	
	@Override
	DuplexChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead);
	
	@Override
	DuplexChannelConfig setWriteSpinCount(int writeSpinCount);
	
	@Override
	DuplexChannelConfig setAllocator(ByteBufAllocator allocator);
	
	@Override
	DuplexChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator);
	
	@Override
	DuplexChannelConfig setAutoRead(boolean autoRead);
	
	@Override
	DuplexChannelConfig setAutoClose(boolean autoClose);
	
	@Override
	DuplexChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator);
	
	@Override
	DuplexChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark);
}
