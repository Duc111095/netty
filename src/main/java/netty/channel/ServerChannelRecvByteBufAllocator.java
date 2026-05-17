package netty.channel;

public final class ServerChannelRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {
	public ServerChannelRecvByteBufAllocator() {
		super(1, true);
	}
	
	@Override
	public Handle newHandle() {
		return new MaxMessageHandle() {
			@Override
			public int guess() {
				return 128;
			}
		};
	}
}
