package netty.buffer;

import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.Recycler.Handle;

abstract class AbstractPooledDerivedByteBuf extends AbstractReferenceCountedByteBuf {
	private final EnhancedHandle<AbstractPooledDerivedByteBuf> recyclerHandle;
	private AbstractByteBuf rootParent;
	
	private ByteBuf parent;
	
	@SuppressWarnings("unchecked")
	AbstractPooledDerivedByteBuf(Handle<? extends AbstractPooledDerivedByteBuf> recyclerHandle) {
		super(0);
		this.recyclerHandle = (EnhancedHandle<AbstractPooledDerivedByteBuf>) recyclerHandle;
	}
	
	final void parent(ByteBuf newParent) {
		assert newParent instanceof SimpleLeakAwareByteBuf;
		parent = newParent;
	}
}
