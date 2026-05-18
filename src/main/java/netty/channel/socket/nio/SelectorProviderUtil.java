package netty.channel.socket.nio;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.StandardProtocolFamily;
import java.nio.channels.spi.SelectorProvider;

import netty.channel.Channel;
import netty.channel.socket.SocketProtocolFamily;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

final class SelectorProviderUtil {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(SelectorProviderUtil.class);
	
	static Method findOpenMethod(String methodName) {
		if (PlatformDependent.javaVersion() >= 15) {
			try {
				return SelectorProvider.class.getMethod(methodName, java.net.ProtocolFamily.class);
			} catch (Throwable t) {
				logger.debug("SelectorProvider.{}(ProtocolFamily) not available, will use default", methodName, t);
			}
		}
		return null;
	}
	
	private static <C extends Channel> C newChannel(Method method, SelectorProvider provider,
			Object family) throws IOException {
		if (family != null && method != null) {
			try {
				@SuppressWarnings("unchecked")
				C channel = (C) method.invoke(provider, family);
				return channel;
			} catch (InvocationTargetException | IllegalAccessException e) {
				throw new IOException(e);
			}
		}
		return null;
	}
	
	static <C extends Channel> C newChannel(Method method, SelectorProvider provider,
			SocketProtocolFamily family) throws IOException {
		if (family != null) {
			return newChannel(method, provider, family.toJdkFamily());
		}
		
		return null;
	}
	
	static <C extends Channel> C newDomainSocketChannel(Method method, SelectorProvider provider) throws IOException {
		return newChannel(method, provider, StandardProtocolFamily.valueOf("UNIX"));
	}
	
	private SelectorProviderUtil() {}
}
