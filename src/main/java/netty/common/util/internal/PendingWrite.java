package netty.common.util.internal;

import netty.common.util.Recycler;
import netty.common.util.Recycler.Handle;
import netty.common.util.ReferenceCountUtil;
import netty.common.util.concurrent.Promise;

public final class PendingWrite {
	
	private static final Recycler<PendingWrite> RECYCLER = 
			new Recycler<PendingWrite>() {
		@Override
		protected PendingWrite newObject(Handle<PendingWrite> handle) {
			return new PendingWrite(handle);
		}
	};
	
	public static PendingWrite newInstance(Object msg, Promise<Void> promise) { 
		PendingWrite pending = RECYCLER.get();
		pending.msg = msg;
		pending.promise = promise;
		return pending;
	}
	
	private final Handle<PendingWrite> handle;
	private Object msg;
	private Promise<Void> promise;

	private PendingWrite(Handle<PendingWrite> handle) {
		this.handle = handle;
	}
	
	public boolean recycle() {
		msg = null;
		promise = null;
		handle.recycle(this);
		return true;
	}
	
	public boolean failAndRecycle(Throwable cause) {
		ReferenceCountUtil.release(msg);
		if (promise != null) {
			promise.setFailure(cause);
		}
		return recycle();
	}
	
	public boolean successAndRecycle() {
		if (promise != null) {
			promise.setSuccess(null);
		}
		return recycle();
	}
	
	public Object msg() {
		return msg;
	}
	
	public Promise<Void> promise() {
		return promise;
	}
	
	public Promise<Void> recycleAndGet() {
		Promise<Void> promise = this.promise;
		recycle();
		return promise;
	}
}
