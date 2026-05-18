package netty.channel.socket;

import netty.buffer.ByteBufAllocator;
import netty.channel.ChannelConfig;
import netty.channel.MessageSizeEstimator;
import netty.channel.RecvByteBufAllocator;
import netty.channel.WriteBufferWaterMark;

public interface ServerSocketChannelConfig extends ChannelConfig {
	int getBacklog();
	
	ServerSocketChannelConfig setBacklog(int backlog);
	
	boolean isReuseAddress();
	
	ServerSocketChannelConfig setReuseAddress(boolean reuseAddress);
	
	int getReceiveBufferSize();
	
	ServerSocketChannelConfig setReceiveBufferSize(int receiveBufferSize);
	
	ServerSocketChannelConfig setPerformancePreferences(int connectionTimes, int latency, int bandwidth);

	@Override
    ServerSocketChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis);

    @Override
    ServerSocketChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead);

    @Override
    ServerSocketChannelConfig setWriteSpinCount(int writeSpinCount);

    @Override
    ServerSocketChannelConfig setAllocator(ByteBufAllocator allocator);

    @Override
    ServerSocketChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator);

    @Override
    ServerSocketChannelConfig setAutoRead(boolean autoRead);

    @Override
    ServerSocketChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator);

    @Override
    ServerSocketChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark);

    @Override
    ServerSocketChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark);

    @Override
    ServerSocketChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark);
}
