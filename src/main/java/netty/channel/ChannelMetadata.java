package netty.channel;

import static netty.common.util.internal.ObjectUtil.checkPositive;

public final class ChannelMetadata {
	private final boolean hasDisconnect;
	private final int defaultMaxMessagesPerRead;
	
	public ChannelMetadata(boolean hasDisconnect) {
		this(hasDisconnect, 16);
	}
	
	public ChannelMetadata(boolean hasDisconnect, int defaultMaxMessagesPerRead) {
		checkPositive(defaultMaxMessagesPerRead, "defaultMaxMessagesPerRead");
		this.hasDisconnect = hasDisconnect;
		this.defaultMaxMessagesPerRead = defaultMaxMessagesPerRead;
	}
	
	public boolean hasDisconnect() {
		return hasDisconnect;
	}
	
	public int defaultMaxMessagesPerRead() {
		return defaultMaxMessagesPerRead;
	}
}
