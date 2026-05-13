package netty.channel;

public interface MaxMessagesRecvByteBufAllocator extends RecvByteBufAllocator {
	int maxMessagesPerRead();
	
	MaxMessagesRecvByteBufAllocator maxMessagesRecvByteBufAllocator(int maxMessagesPerRead);
}
