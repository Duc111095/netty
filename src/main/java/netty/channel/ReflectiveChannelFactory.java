package netty.channel;

import java.lang.reflect.Constructor;

import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.StringUtil;

public class ReflectiveChannelFactory<T extends Channel> implements ChannelFactory<T>{
	private final Constructor<? extends T> constructor;
	
	public ReflectiveChannelFactory(Class<? extends T> clazz) {
		ObjectUtil.checkNotNull(clazz, "clazz");
		try {
			this.constructor = clazz.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new IllegalStateException("Class " + StringUtil.simpleClassName(clazz) +
					" does not have a public non-arg constructor", e); 
		}
	}
	
	@Override
	public T newChannel() {
		try {
			return constructor.newInstance();
		} catch (Throwable t) {
			throw new ChannelException("Unable to create Channel from class " + constructor.getDeclaringClass());
		}
	}
	
	@Override
	public String toString() {
		return StringUtil.simpleClassName(ReflectiveChannelFactory.class) + 
				'(' + StringUtil.simpleClassName(constructor.getDeclaringClass()) + ".class)"; 
	}
}
