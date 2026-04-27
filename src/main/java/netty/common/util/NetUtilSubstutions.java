package netty.common.util;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collection;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.InjectAccessors;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(NetUtil.class)
final class NetUtilSubstitutions {
	
	private NetUtilSubstitutions() {
		
	}
	
	@Alias
	@InjectAccessors(NetUtilLocalhost4Accessor.class)
	public static Inet4Address LOCALHOST4;
	
	@Alias
	@InjectAccessors(NetUtilLocalhost6Accessor.class)
	public static Inet6Address LOCALHOST6;
	
	@Alias
	@InjectAccessors(NetUtilLocalhostAccessor.class)
	public static InetAddress LOCALHOST;
	
	@Alias
	@InjectAccessors(NetUtilNetworkInterfacesAccessor.class)
	public static Collection<NetworkInterface> NETWORK_INTERFACES;

	private static final class NetUtilLocalhost4Accessor {
		static Inet4Address get() {
			return NetUtilLocalhost4LazyHolder.LOCALHOST4;
		}
		
		static void set(Inet4Address ignore) {
			
		}
	}
	
	private static final class NetUtilLocalhost4LazyHolder {
		private static final Inet4Address LOCALHOST4 = NetUtilInitializations.createLocalhost4();
	}
	
	private static final class NetUtilLocalhost6Accessor {
		static Inet6Address get() {
			return NetUtilLocalhost6LazyHolder.LOCALHOST6;
		}
		
		static void set(Inet6Address ignore) {
			
		}
	}
	
	private static final class NetUtilLocalhost6LazyHolder {
		private static final Inet6Address LOCALHOST6 = NetUtilInitializations.createLocalhost6();
	}
	
	private static final class NetUtilLocalhostAccessor {
		static InetAddress get() {
			return NetUtilLocalhostLazyHolder.LOCALHOST;
		}
		
		static void set(InetAddress ignore) {
			
		}
	}
	
	private static final class NetUtilLocalhostLazyHolder {
		private static final InetAddress LOCALHOST = NetUtilInitializations
				.determineLoopback(NetUtilNetworkInterfacesLazyHolder.NETWORK_INTERFACES,
						NetUtilLocalhost4LazyHolder.LOCALHOST4, NetUtilLocalhost6LazyHolder.LOCALHOST6)
				.address();
	}
	
	private static final class NetUtilNetworkInterfacesAccessor {
		static Collection<NetworkInterface> get() {
			return NetUtilNetworkInterfacesLazyHolder.NETWORK_INTERFACES;
		}
		
		static void set(Collection<NetworkInterface> ignored) {
			
		}
	}
	
	private static final class NetUtilNetworkInterfacesLazyHolder {
		private static final Collection<NetworkInterface> NETWORK_INTERFACES = 
				NetUtilInitializations.networkInterfaces();
	}
}
