package netty.common.util;

import netty.common.util.internal.RefCnt;

public abstract class AbstractReferenceCounted implements ReferenceCounted {
	
	private final RefCnt refCnt = new RefCnt();
	
	@Override
	public int refCnt() {
		return RefCnt.refCnt(refCnt);
	}
	
	protected void setRefCnt(int refCnt) {
		RefCnt.setRefCnt(this.refCnt, refCnt);
	}
	
	@Override
	public ReferenceCounted retain() {
		RefCnt.retain(refCnt);
		return this;
	}
	
	@Override
	public ReferenceCounted retain(int increment) {
		RefCnt.retain(refCnt, increment);
		return this;
	}
	
	@Override
	public ReferenceCounted touch() {
		return touch(null);
	}
	
	@Override
	public boolean release() {
		return handleRelease(RefCnt.release(refCnt));
	}
	
	@Override
	public boolean release(int decrement) {
		return handleRelease(RefCnt.release(refCnt, decrement));
	}
	
	private boolean handleRelease(boolean result) {
		if (result ) {
			deallocate();
		}
		return result;
	}
	
	protected abstract void deallocate();
}
