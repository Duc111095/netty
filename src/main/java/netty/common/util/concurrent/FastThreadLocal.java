package netty.common.util.concurrent;

import netty.common.util.internal.InternalThreadLocalMap;
import netty.common.util.internal.PlatformDependent;

import static netty.common.util.internal.InternalThreadLocalMap.UNSET;
import static netty.common.util.internal.InternalThreadLocalMap.VARIABLES_TO_REMOVE_INDEX;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;


public class FastThreadLocal<V> {
	public static void removeAll() {
		InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
		if (threadLocalMap == null) {
			return;
		}
		
		try {
			Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
			if (v != null && v != InternalThreadLocalMap.UNSET) {
				@SuppressWarnings("unchecked")
				Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v; 
				FastThreadLocal<?>[] variablesToRemoveArray =
						variablesToRemove.toArray(new FastThreadLocal[0]);
				for (FastThreadLocal<?> tlv : variablesToRemoveArray) {
					tlv.remove(threadLocalMap);
				}
			}
		} finally {
			InternalThreadLocalMap.remove();
		}
	}
	
	public static int size() {
		InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
		if (threadLocalMap == null) {
			return 0;
		} else {
			return threadLocalMap.size();
		}
	}
	
	public static void destroy() {
		InternalThreadLocalMap.destroy();
	}
	
	@SuppressWarnings("unchecked")
	private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
		Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
		Set<FastThreadLocal<?>> variablesToRemove;
		if (v == InternalThreadLocalMap.UNSET || v == null) {
			variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<FastThreadLocal<?>, Boolean>());
			threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
		} else {
			variablesToRemove = (Set<FastThreadLocal<?>>) v;
		}
		variablesToRemove.add(variable);
	}
	
	private static void removeFromVariablesToRemove(
			InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
		Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
		if (v == InternalThreadLocalMap.UNSET || v == null) {
			return;
		}
		@SuppressWarnings("unchecked")
		Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
		variablesToRemove.remove(variable);
	}
	
	private final int index;
	public FastThreadLocal() {
		index = InternalThreadLocalMap.nextVariableIndex();
	}
	
	@SuppressWarnings("unchecked")
	public final V get() {
		InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
		Object v = threadLocalMap.indexedVariable(index);
		if (v != InternalThreadLocalMap.UNSET) {
			return (V) v;
		}
		return initialize(threadLocalMap);
	}
	
	@SuppressWarnings("unchecked")
	public final V getIfExists() {
		InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
		if (threadLocalMap != null) {
			Object v = threadLocalMap.indexedVariable(index);
			if (v != InternalThreadLocalMap.UNSET) {
				return (V) v;
			}
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public final V get(InternalThreadLocalMap threadLocalMap) {
		Object v = threadLocalMap.indexedVariable(index);
		if (v != InternalThreadLocalMap.UNSET) {
			return (V) v;
		}
		return initialize(threadLocalMap);
	}
	
	private V initialize(InternalThreadLocalMap threadLocalMap) {
		V v = null;
		try {
			v = initialValue();
			if (v == InternalThreadLocalMap.UNSET) {
				throw new IllegalArgumentException("InternalThreadLocalMap.UNSET can not be initial value.");
			}
		} catch (Exception e) {
			PlatformDependent.throwException(e);
		}
		
		threadLocalMap.setIndexedVariable(index, v);
		addToVariablesToRemove(threadLocalMap, this);
		return v;
	}
	
	public final void set(V value) {
		getAndSet(value);
	}
	
	public final void set(InternalThreadLocalMap threadLocalMap, V value) {
		getAndSet(threadLocalMap, value);
	}
	
	public V getAndSet(V value) {
		if (value != InternalThreadLocalMap.UNSET) {
			InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
			return setKnownNotUnset(threadLocalMap, value);
		}
		return removeAndGet(InternalThreadLocalMap.getIfSet());
	}
	
	public V getAndSet(InternalThreadLocalMap threadLocalMap, V value) {
		if (value != InternalThreadLocalMap.UNSET) {
			return setKnownNotUnset(threadLocalMap, value);
		}
		return removeAndGet(threadLocalMap);
	}
	
	@SuppressWarnings("unchecked")
	public V setKnownNotUnset(InternalThreadLocalMap threadLocalMap, V value) {
		V old = (V) threadLocalMap.getAndSetIndexedVariable(index, value);
		if (old == UNSET) {
			addToVariablesToRemove(threadLocalMap, this);
			return null;
		}
		return old;
	}
	
	public final boolean isSet() {
		return isSet(InternalThreadLocalMap.getIfSet());
	}
	
	public final boolean isSet(InternalThreadLocalMap threadLocalMap) {
		return threadLocalMap != null && threadLocalMap.isIndexedVariableSet(index);
	}
	
	public final void remove() {
		remove(InternalThreadLocalMap.getIfSet());
	}
	
	public final void remove(InternalThreadLocalMap threadLocalMap) {
		removeAndGet(threadLocalMap);
	}
	
	@SuppressWarnings("unchecked")
	private V removeAndGet(InternalThreadLocalMap threadLocalMap) {
		if (threadLocalMap == null) {
			return null;
		}
		
		Object v = threadLocalMap.removeIndexedVariable(index);
		if (v != InternalThreadLocalMap.UNSET) {
			removeFromVariablesToRemove(threadLocalMap, this);
			try {
				onRemoval((V) v);
			} catch (Exception e) {
				PlatformDependent.throwException(e);
			}
			return (V) v;
		}
		return null;
	}
	
	protected V initialValue() throws Exception {
		return null;
	}
	
	protected void onRemoval(@SuppressWarnings("UnusedParameters") V value) throws Exception {}
}
