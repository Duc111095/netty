package netty.buffer;

import java.nio.ByteBuffer;

import netty.common.util.internal.PlatformDependent;

final class VarHandleByteBufferAccess {
	
	private VarHandleByteBufferAccess() {
		
	}
	
	static short getShortBE(ByteBuffer buffer, int index) {
		return (short) PlatformDependent.shortBeByteBufferView().get(buffer, index);
	}
	
	static void setShortBE(ByteBuffer buffer, int index, int value) {
		PlatformDependent.shortBeByteBufferView().set(buffer, index, (short) value);
	}
	
	static short getShortLE(ByteBuffer buffer, int index) {
		return (short) PlatformDependent.shortLeByteBufferView().get(buffer, index);
	}
	
	static void setShortLE(ByteBuffer buffer, int index, int value) {
		PlatformDependent.shortLeByteBufferView().set(buffer, index, (short) value);
	}
	
	static int getIntBE(ByteBuffer buffer, int index) {
		return (int) PlatformDependent.intBeByteBufferView().get(buffer, index);
	}
	
	static void setIntBE(ByteBuffer buffer, int index, int value) {
		PlatformDependent.intBeByteBufferView().set(buffer, index, value);
	}
	
	static int getIntLE(ByteBuffer buffer, int index) {
		return (int) PlatformDependent.intLeByteBufferView().get(buffer, index);
	}
	
	static void setIntLE(ByteBuffer buffer, int index, int value) {
		PlatformDependent.intLeByteBufferView().set(buffer, index, value);
	}
	
	static long getLongBE(ByteBuffer buffer, int index) {
		return (long) PlatformDependent.longBeByteBufferView().get(buffer, index);
	}
	
	static void setLongBE(ByteBuffer buffer, int index, int value) {
		PlatformDependent.longBeByteBufferView().set(buffer, index, value);
	}
	
	static long getLongLE(ByteBuffer buffer, int index) {
		return (long) PlatformDependent.longLeByteBufferView().get(buffer, index);
	}
	
	static void setLongLE(ByteBuffer buffer, int index, long value) {
		PlatformDependent.longLeByteBufferView().set(buffer, index, value);
	}
	
	static short getShortBE(byte[] memory, int index) {
		return (short) PlatformDependent.shortBeArrayView().get(memory, index);
	}
	
	static void setShortBE(byte[] memory, int index, int value) {
		PlatformDependent.shortBeArrayView().set(memory, index, value);
	}
	
	static short getShortLE(byte[] memory, int index) {
		return  (short) PlatformDependent.shortLeArrayView().get(memory, index);
	}
	
	static void setShortLE(byte[] memory, int index, int value) {
		PlatformDependent.shortLeArrayView().set(memory, index, value);
	}
	
	static int getIntBE(byte[] memory, int index) {
        return (int) PlatformDependent.intBeArrayView().get(memory, index);
    }
	
	static void setIntBE(byte[] memory, int index, int value) {
        PlatformDependent.intBeArrayView().set(memory, index, value);
    }

    static int getIntLE(byte[] memory, int index) {
        return (int) PlatformDependent.intLeArrayView().get(memory, index);
    }

    static void setIntLE(byte[] memory, int index, int value) {
        PlatformDependent.intLeArrayView().set(memory, index, value);
    }

    static long getLongBE(byte[] memory, int index) {
        return (long) PlatformDependent.longBeArrayView().get(memory, index);
    }

    static void setLongBE(byte[] memory, int index, long value) {
        PlatformDependent.longBeArrayView().set(memory, index, value);
    }

    static long getLongLE(byte[] memory, int index) {
        return (long) PlatformDependent.longLeArrayView().get(memory, index);
    }

    static void setLongLE(byte[] memory, int index, long value) {
        PlatformDependent.longLeArrayView().set(memory, index, value);
    }
}
