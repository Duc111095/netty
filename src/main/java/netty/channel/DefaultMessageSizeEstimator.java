package netty.channel;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufHolder;
import netty.common.util.internal.ObjectUtil;

public final class DefaultMessageSizeEstimator implements MessageSizeEstimator {
	
	private static final class HandleImpl implements Handle {
		private final int unknownSize;
		
		private HandleImpl(int unknownSize) {
			this.unknownSize = unknownSize;
		}
		
		@Override
		public int size(Object msg) {
			if (msg instanceof ByteBuf) {
				return ((ByteBuf) msg).readableBytes();
			}
			if (msg instanceof ByteBufHolder) {
				return ((ByteBufHolder) msg).content().readableBytes();
			}
			if (msg instanceof FileRegion) {
				return 0;
			}
			return unknownSize;
		}
	}
	
	public static final MessageSizeEstimator DEFAULT = new DefaultMessageSizeEstimator(8);
	
	private final Handle handle;
	
	public DefaultMessageSizeEstimator(int unknownSize) {
		ObjectUtil.checkPositiveOrZero(unknownSize, "unknownSize");
		handle = new HandleImpl(unknownSize);
	}
	
	@Override
	public Handle newHandle()  {
		return handle;
	}
}
