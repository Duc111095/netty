package netty.channel;

import netty.buffer.ByteBuf;
import netty.buffer.ByteBufAllocator;
import netty.buffer.CompositeByteBuf;
import netty.buffer.Unpooled;
import netty.common.util.internal.ObjectUtil;

public final class CoalescingBufferQueue extends AbstractCoalescingBufferQueue {
	private final Channel channel;
	
	public CoalescingBufferQueue(Channel channel) {
		this(channel, 4);
	}
	
	public CoalescingBufferQueue(Channel channel, int initSize) {
		this(channel, initSize, false);
	}
	
	public CoalescingBufferQueue(Channel channel, int initSize, boolean updateWritability) {
		super(updateWritability ? channel : null, initSize);
		this.channel = ObjectUtil.checkNotNull(channel, "channel");
	}
	
	public ByteBuf remove(int bytes, ChannelPromise aggregatePromise) {
		return remove(channel.alloc(), bytes, aggregatePromise);
	}
	
	public void releaseAndFailAll(Throwable cause) {
		releaseAndFailAll(channel, cause);
	}
	
	@Override
	protected ByteBuf compose(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf next) {
		if (cumulation instanceof CompositeByteBuf) {
			CompositeByteBuf composite = (CompositeByteBuf) cumulation;
			composite.addComponent(true, next);
			return composite;
		}
		return composeIntoComposite(alloc, cumulation, next);
	}
	
	@Override
	protected ByteBuf removeEmptyValue() {
		return Unpooled.EMPTY_BUFFER;
	}
}
