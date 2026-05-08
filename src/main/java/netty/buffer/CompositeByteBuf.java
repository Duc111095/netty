package netty.buffer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import netty.common.util.internal.ObjectUtil;

public class CompositeByteBuf extends AbstractReferenceCountedByteBuf implements Iterable<ByteBuf> {
	
	private static final ByteBuffer EMPTY_NIO_BUFFER = Unpooled.EMPTY_BUFFER.nioBuffer();
	private static final Iterator<ByteBuf> EMPTY_ITERATOR = Collections.<ByteBuf>emptyList().iterator();
	
	private final ByteBufAllocator alloc;
	private final boolean direct;
	private final int maxNumComponents;
	
	private int componentCount;
	private Component[] components;
	
	private CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents, int initSize) {
		super(AbstractByteBufAllocator.DEFAULT_MAX_CAPACITY);
		
		this.alloc = ObjectUtil.checkNotNull(alloc, "alloc");
		if (maxNumComponents < 1) {
			throw new IllegalArgumentException(
					"maxNumComponents: " + maxNumComponents + " (expected: >= 1");
		}
		this.direct = direct;
		this.maxNumComponents = maxNumComponents;
		components = newCompArray(initSize, maxNumComponents);
	}
	
	public CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents) {
		this(alloc, direct, maxNumComponents, 0);
	}
	
	public CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents, ByteBuf... buffers) {
		this(alloc, direct, maxNumComponents, buffers, 0);
	}
	
	CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents,
			ByteBuf[] buffers, int offset) {
		this(alloc, direct, maxNumComponents, buffers.length - offset);
		
		addComponents0(false, 0, buffers, offset);
		consolidateIfNeeded();
		setIndex0(0, capacity());
	}
	
	public CompositeByteBuf(
			ByteBufAllocator alloc, boolean direct, int maxNumComponents, Iterable<ByteBuf> buffers) {
		this(alloc, direct, maxNumComponents,
				buffers instanceof Collection ? ((Collection<ByteBuf>) buffers).size() : 0);
		
		addComponent0(false, 0, buffers);
		setIndex(0, capacity());
	}
	
	interface ByteWrapper<T> {
		ByteBuf wrap(T bytes);
		boolean isEmpty(T bytes);
	}
	
	static final ByteWrapper<byte[]> BYTE_ARRAY_WRAPPER = new ByteWrapper<byte[]>() {
		@Override
		public ByteBuf wrap(byte[] bytes) {
			return Unpooled.wrappedBuffer(bytes);
		}
		@Override
		public boolean isEmpty(byte[] bytes) {
			return bytes.length == 0;
		}
	};
	
	static final ByteWrapper<ByteBuffer> BYTE_BUFFER_WRAPPER = new ByteWrapper<ByteBuffer>() {
		@Override
		public ByteBuf wrap(ByteBuffer bytes) {
			return Unpooled.wrappedBuffer(bytes);
		}
		@Override
		public boolean isEmpty(ByteBuffer bytes) {
			return !bytes.hasRemaining();
		}
	};
	
	<T> CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents,
			ByteWrapper<T> wrapper, T[] buffers, int offset) {
		this(alloc, direct, maxNumComponents, buffers.length - offset);
		
		addComponents0(false, 0, wrapper, buffers, offset);
		consolidateIfNeeded();
		setIndex(0, capacity());
	}
	
	private static Component[] newCompArray(int initComponents, int maxNumComponents) {
		int capacityGuess = Math.min(AbstractByteBufAllocator.DEFAULT_MAX_COMPONENTS, maxNumComponents);
		return new Component[Math.max(initComponents, capacityGuess)];
	}
	
	CompositeByteBuf(ByteBufAllocator alloc) {
		super(Integer.MAX_VALUE);
		this.alloc = alloc;
		direct = false;
		maxNumComponents = 0;
		components = null;
	}
	
	public CompositeByteBuf addComponent(ByteBuf buffer) {
		return addComponent(false, buffer);
	}
	
	public CompositeByteBuf addComponents(ByteBuf... buffers) {
		return addComponents(false, buffers);
	}
	
	public CompositeByteBuf addComponents(Iterable<ByteBuf> buffers) {
		return addComponents(false, buffers);
	}
	
	public CompositeByteBuf addComponent(int cIndex, ByteBuf buffer) {
		return addComponent(false, cIndex, buffer);
	}
	
	public CompositeByteBuf addComponent(boolean increaseWriterIndex, ByteBuf buffer) {
		return addComponent(increaseWriterIndex, componentCount, buffer);
	}
	
	public CompositeByteBuf addComponents(boolean increaseWriterIndex, ByteBuf... buffers) {
		checkNotNull(buffers, "buffers");
		addComponents0(increaseWriterIndex, componentCount, buffers, 0);
		consolidateIfNeeded();
		return this;
	}
	
	public CompositeByteBuf addComponents(boolean increaseWriterIndex, Iterable<ByteBuf> buffers) {
		return addComponents(increaseWriterIndex, componentCount, buffers);
	}
	
	public CompositeByteBuf addComponent(boolean increaseWriterIndex, int cIndex, ByteBuf buffer) {
		checkNotNull(buffer, "buffer");
		addComponents0(increaseWriterIndex, cIndex, buffer);
		consolidateIfNeeded();
		return this;
	}
	
	private static void checkForOverflow(int capacity, int readableBytes) {
		if (capacity + readableBytes < 0) {
			throw new IllegalArgumentException("can't increase by " + readableBytes + " as capacity(" + capacity + ")" + 
					" would overflow " + Integer.MAX_VALUE);
		}
	}
	
	private static final class Component {
		final ByteBuf srcBuf;
		final ByteBuf buf;
		final AbstractByteBuf abuf;
		
		int srcAdjustment;
		int adjustment;
		
		int offset;
		int endOffset;
		
		private ByteBuf slice;
		
		Component(ByteBuf srcBuf, int srcOffset, ByteBuf buf, int bufOffset,
				int offset, int len, ByteBuf slice) {
			this.srcBuf = srcBuf;
			this.srcAdjustment = srcOffset - offset;
			this.buf = buf;
			this.abuf = buf instanceof AbstractByteBuf ? (AbstractByteBuf) buf : null;
			this.adjustment = bufOffset - offset;
			this.offset = offset;
			this.endOffset = offset + len;
			this.slice = slice;
		}
		
		int srcIdx(int index) {
			return index + srcAdjustment;
		}
		
		int idx(int index) {
			return index + adjustment;
		}
		
		int length() {
			return endOffset - offset;
		}
		
		void reposition(int newOffset) {
			int move = newOffset - offset;
			endOffset += move;
			srcAdjustment -= move;
			adjustment -= move;
			offset = newOffset;
		}
		
		void transferTo(ByteBuf dst) {
			dst.writeBytes(buf, idx(offset), length());
			free();
		}
		
		ByteBuf slice() {
			ByteBuf s = slice;
			if (s == null) {
				slice = s = srcBuf.slice(srcIdx(offset), length());
			}
			return s;
		}
		
		ByteBuf duplicate() {
			return srcBuf.duplicate();
		}
		
		ByteBuffer internalNioBuffer(int index, int length) {
			return srcBuf.internalNioBuffer(srcIdx(index), length);
		}
		
		void free() {
			slice = null;
			srcBuf.release();
		}
	}
}
