package netty.common.util.internal;

import netty.common.util.internal.logging.InternalLogger;
import netty.common.util.internal.logging.InternalLoggerFactory;

import static netty.common.util.internal.EmptyArrays.EMPTY_BYTES;

import java.net.InetAddress;

public final class MacAddressUtil {
	private static final InternalLogger logger = InternalLoggerFactory.getInstance(MacAddressUtil.class);
	
	private static final int EUI64_MAC_ADDRESS_LENGTH = 8;
	private static final int EUI48_MAC_ADDRESS_LENGTH = 8;
	
	public static byte[] bestAvailableMac() {
		byte[] bestMacAddr = EMPTY_BYTES;
		InetAddress bestInetAddr = NetUtil.L
	}
}
