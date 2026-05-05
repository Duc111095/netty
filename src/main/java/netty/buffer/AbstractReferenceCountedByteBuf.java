package netty.buffer;

import netty.common.util.internal.RefCnt;

public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
	private final RefCnt refCnt = new RefCnt();
	
	protected AbstractReferenceCountedByteBuf(int maxCapacity) {
		super(maxCapacity);
	}
	
	@Override
	boolean isAccessible() {
		return RefCnt.isLiveNonVolatile(refCnt);
	}
	
	@Override
	public int refCnt() {
		return RefCnt.refCnt(refCnt);
	}
	
	protected final void setRefCnt(int count) {
		RefCnt.setRefCnt(refCnt, count);
	}
	
	protected final void resetRefCnt() {
		RefCnt.resetRefCnt(refCnt);
	}
	
	@Override
	public ByteBuf retain() {
		RefCnt.retain(refCnt);
		return this;
	}
	
	@Override
	public ByteBuf retain(int increment) {
		RefCnt.retain(refCnt, increment);
		return this;
	}
	
	@Override
	public ByteBuf touch() {
		return this;
	}
	
	@Override
	public ByteBuf touch(Object hint) {
		return this;
	}
	
	@Override
	public boolean release() {
		return handleRelease(RefCnt.release(refCnt));
	}
	
	private boolean handleRelease(boolean result) {
		if (result) {
			deallocate();
		}
		return result;
	}
	
	protected abstract void deallocate();
}
