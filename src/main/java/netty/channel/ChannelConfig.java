package netty.channel;

import java.util.Map;

import netty.buffer.ByteBufAllocator;

public interface ChannelConfig {
	
	Map<ChannelOption<?>, Object> getOptions();
	
	boolean setOptions(Map<ChannelOption<?>, ?> options);
	
	<T> T getOption(ChannelOption<T> option);
	
	<T> boolean setOption(ChannelOption<T> option, T value);
	
	int getConnectTimeoutMillis();
	
	ChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis);
	
	int getMaxMessagesPerRead();
	
	ChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead);
	
	int getWriteSpinCount();
	
	ChannelConfig setWriteSpinCount(int writeSpinCount);
	
	ByteBufAllocator getAllocator();
	
	ChannelConfig setAllocator(ByteBufAllocator allocator);
	
	<T extends RecvByteBufAllocator> T getRecvByteBufAllocator();
	
	ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator);
	
	<T extends RecvByteBufAllocator> T getRecvByteBufAllocator();
	
	ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator);
	
	boolean isAutoRead();
	
	ChannelConfig setAutoRead(boolean autoRead);
	
	boolean isAutoClose();
	
	ChannelConfig setAutoClose(boolean autoClose);
	
	int getWriteBufferHighWaterMark();
	
	ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark);
	
	int getWriteBufferLowWaterMark();
	
	ChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark);
	
	MessageSizeEstimator getMessageSizeEstimator(MessageSizeEstimator estimator);
	
	WriteBufferWaterMark getWriteBufferWaterMark();
	
	ChannelConfig setWriterBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark);
}
