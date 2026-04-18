package netty.common.util.internal;

import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;

public class EmptyArrays {
	public static final int[] EMPTY_INTS = {};
    public static final byte[] EMPTY_BYTES = {};
    public static final char[] EMPTY_CHARS = {};
    public static final Object[] EMPTY_OBJECTS = {};
    public static final Class<?>[] EMPTY_CLASSES = {};
    public static final String[] EMPTY_STRINGS = {};
    public static final AsciiString[] EMPTY_ASCII_STRINGS = {};
    public static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    public static final ByteBuffer[] EMPTY_BYTE_BUFFERS = {};
    public static final Certificate[] EMPTY_CERTIFICATES = {};
    public static final X509Certificate[] EMPTY_X509_CERTIFICATES = {};
    public static final javax.security.cert.X509Certificate[] EMPTY_JAVAX_X509_CERTIFICATES = {};
    @SuppressWarnings("rawtypes")
    public static final Map.Entry[] EMPTY_MAP_ENTRY = {};

    public static final Throwable[] EMPTY_THROWABLES = {};

    private EmptyArrays() { }
}
