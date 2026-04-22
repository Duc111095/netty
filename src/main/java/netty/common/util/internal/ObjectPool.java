package netty.common.util.internal;

import netty.common.util.Recycler;

/**
 * Light weight object pool.
 * @param <T> the type of the pooled object
 */
public abstract class ObjectPool<T>{

	ObjectPool() {}
	
	public abstract T get();
	
	public interface Handle<T> {
		void recycle(T self);
	}
	
	public interface ObjectCreator<T> {
		T newObject(Handle<T> handle);
	}
	
	public static <T> ObjectPool<T> newPool(final ObjectCreator<T> creator) {
		return new RecyclerObjectPool<T>(ObjectUtil.checkNotNull(creator, "creator"));
	}
	
	private static final class RecyclerObjectPool<T> extends ObjectPool<T> {
		private final Recycler<T> recycler;
		
		RecyclerObjectPool(final ObjectCreator<T> creator) {
			recycler = new Recycler<T>() {
				@Override
				protected T newObject(Handle<T> handle) {
					return creator.newObject(handle);
				}
			};
		}
		
		@Override
		public T get() {
			return recycler.get();
		}
	}
}