package netty.channel;

import netty.common.util.internal.ObjectUtil;

public class FixedRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {
	private final int bufferSize;
	
	private final class HandleImpl extends MaxMessageHandle {
		private final int bufferSize;
		
		HandleImpl(int bufferSize) {
			this.bufferSize = bufferSize;
		}
		
		@Override
		public int guess() {
			return bufferSize;
		}
	}
	
	public FixedRecvByteBufAllocator(int bufferSize) {
		ObjectUtil.checkPositive(bufferSize, "bufferSize");
		this.bufferSize = bufferSize;
	}
	
	@Override
	public Handle newHandle() {
		return new HandleImpl(bufferSize);
	}
	
	@Override
	public FixedRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
		super.respectMaybeMoreData(respectMaybeMoreData);
		return this;
	}
}
