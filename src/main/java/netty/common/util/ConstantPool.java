package netty.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static netty.common.util.internal.ObjectUtil.checkNotNull;
import static netty.common.util.internal.ObjectUtil.checkNonEmpty;


public abstract class ConstantPool<T extends Constant<T>> {
	
	private final ConcurrentMap<String, T> constants = new ConcurrentHashMap<>();
	private final AtomicInteger nextId = new AtomicInteger(1);
	
	public T valueOf(Class<?> firstNameComponent, String secondNameComponent) {
		return valueOf(
				checkNotNull(firstNameComponent, "firstNameComponent").getName() + 
				'#' + 
				checkNotNull(secondNameComponent, "secondNameComponent"));
	}
	
	public T valueOf(String name) {
		return getOrCreate(checkNonEmpty(name, "name"));
	}
	
	private T getOrCreate(String name) {
		T constant = constants.get(name);
		if (constant == null) {
			final T tempConstant = newConstant(nextId(), name);
			constant = constants.putIfAbsent(name, tempConstant);
			if (constant == null) {
				return tempConstant;
			}
		}
		return constant;
	}
	
	public boolean exists(String name)  {
		return constants.containsKey(checkNonEmpty(name, "name"));
	}
	
	public T newInstance(String name) {
		return createOrThrow(checkNonEmpty(name, "name"));
	}
	
	private T createOrThrow(String name) {
		T constant = constants.get(name);
		if (constant == null) {
			final T tempConstant = newConstant(nextId(), name);
			constant = constants.putIfAbsent(name, tempConstant);
			if (constant == null) {
				return tempConstant;
			}
		}
		throw new IllegalArgumentException(String.format("'%s' is already in use", name));
	}
	
	protected abstract T newConstant(int id, String name);
	
	public final int nextId() {
		return nextId.getAndIncrement();
	}
}
 