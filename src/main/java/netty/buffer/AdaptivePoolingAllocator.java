package netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import netty.common.util.ByteProcessor;
import netty.common.util.CharsetUtil;
import netty.common.util.IllegalReferenceCountException;
import netty.common.util.NettyRuntime;
import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.RefCnt;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.UnstableApi;

@UnstableApi
final class AdaptivePoolingAllocator {
	private static final int LOW_MEM_THRESHOLD = 512 * 1024 * 1024;
	private static final boolean IS_LOW_MEM = Runtime.getRuntime().maxMemory() <= LOW_MEM_THRESHOLD;
	
	private static final boolean DISABLE_THRESH_LOCAL_MAGAZINES_ON_LOW_MEM = SystemPropertyUtil.getBoolean(
			"io.netty.allocator.disableThreadLocalMagazinesOnLowMemory", true);
	
	static final int MIN_CHUNK_SIZE = 128 * 1024;
	private static final int EXPANSION_ATTEMPTS = 3;
	private static final int INITIAL_MAGAZINES = 1;
	private static final int RETIRE_CAPACITY = 256;
	private static final int MAX_STRIPES = IS_LOW_MEM ? 1 : NettyRuntime.availableProcessors()* 2;
	private static final int BUFS_PER_CHUNK = 8;
	
	private static final int MAX_CHUNK_SIZE = IS_LOW_MEM ?
			2 * 1024 * 1024 :
			8 * 1024 * 1024;
	private static final int MAX_POOLED_BUF_SIZE = MAX_CHUNK_SIZE / BUFS_PER_CHUNK;
	
	private static final int CHUNK_REUSE_QUEUE = Math.max(2, SystemPropertyUtil.getInt(
			"io.netty.allocator.chunkReuseQueueCapacity", NettyRuntime.availableProcessors() * 2));
	private static final int MAGAZINE_BUFFER_QUEUE_CAPACITY = SystemPropertyUtil.getInt(
			"io.netty.allocator.magazineBufferQueueCapacity", 1024);
	
	private static final int[] SIZE_CLASSES = {
			32,
			64,
			128,
			256,
			512,
			640,
			1024,
			1152,
			2048,
			2304,
			4096,
			4352,
			8192,
			8704,
			16384,
			16896,
	};
	
	private static final int SIZE_CLASSES_COUNT = SIZE_CLASSES.length;
	private static final byte[] SIZE_INDEXES = new byte[SIZE_CLASSES[SIZE_CLASSES_COUNT - 1] / 32 + 1];
	
	static {
		if (MAGAZINE_BUFFER_QUEUE_CAPACITY < 2) {
			throw new IllegalArgumentException("MAGAZINE_BUFFER_QUEUE_CAPACITY: " + MAGAZINE_BUFFER_QUEUE_CAPACITY 
					+ " (expected: >= " + 2 + ')');
		}
		int lastIndex = 0;
		for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
			int sizeClass = SIZE_CLASSES[i];
			assert (sizeClass & 31) == 0 : "Size class must be a multiple of 32";
			int sizeIndex = sizeIndexOf(sizeClass);
			Arrays.fill(SIZE_INDEXES, lastIndex + 1, sizeIndex + 1, (byte) i);
			lastIndex = sizeIndex;
		}
	}
	
	private final ChunkAllocator chunkAllocator;
	private final ChunkRegistry chunkRegistry;
	private final MagazineGroup[] sizeClassedMagazineGroup;
	private final MagazineGroup largeBufferMagazineGroup;
	private final FastThreadLocal<MagazineGroup[]> threaLocalGroup;
	
	private static class Chunk implements ChunkInfo {
		protected final AbstractByteBuf delegate;
		protected Magazine magazine;
		private final AdaptivePoolingAllocator allocator;
		private final RefCnt refCnt = new RefCnt();
		private final int capacity;
		private final boolean pooled;
		protected int allocatedBytes;
		
		Chunk() {
			delegate = null;
			magazine = null;
			allocator = null;
			capacity = 0;
			pooled = false;
		}
		
		Chunk(AbstractByteBuf delegate, Magazine magazine, boolean pooled) {
			this.delegate = delegate;
			this.pooled = pooled;
			capacity = delegate.capacity();
			attachToMagazine(magazine);
			
			allocator = magazine.group.allocator;
			
			if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
				AllocateChunkEvent event = new AllocateChunkEvent();
				if (event.shouldCommit() ) {
					event.fill(this, AdaptiveByteBufAllocator.class);
					event.pooled = pooled;
					event.threadLocal = magazine.allocationLock == null;
					event.commit();
				}
			}
		}
		
		Magazine currentMagazine() {
			return magazine;
		}
		
		void detachFromMagazine() {
			if (magazine != null) {
				magazine = null;
			}
		}
		
		void attachToMagazine(Magazine magazine) {
			assert this.magazine = null;
			this.magazine = magazine;
		}
		
		void releaseFromMagazine() {
			Magazine mag = magazine;
			detachFromMagazine();
			if (!mag.offerToQueue(this)) {
				markToDeallocate();
			}
		}
		
		void releaseSegment(int ignoredSegmentId, int size) {
			release();
		}
		
		void markToDeallocate() {
			release();
		}
		
		private void retain() {
			RefCnt.retain(refCnt);
		}
		
		protected boolean release() {
			boolean deallocate = RefCnt.release(refCnt);
			if (deallocate) {
				deallocate();
			}
			return deallocate;
		}
		
		protected void deallocate() {
			onRelease();
			allocator.chunkRegistry.remove(this);
			delegate.release();
		}
		
		private void onRelease() {
			if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
				FreeChunkEvent event = new FreeChunkEvent();
				if (event.shouldCommit()) {
					event.fill(this, AdaptiveByteBufAllocator.class);
					event.pooled = pooled;
					event.commit();
				}
			}
		}
		
		public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
			int startIndex = allocatedBytes;
			allocatedBytes = startIndex + startingCapacity;
			Chunk chunk = this;
			chunk.retain();
			try {
				buf.init(delegate, chunk, 0, 0, startIndex, size, startingCapacity, maxCapacity);
				chunk = null;
			} finally {
				if (chunk != null) {
					allocatedBytes = startIndex;
					chunk.release();
				}
			}
			return true;
		}
		
		public int remainingCapacity() {
			return capacity - allocatedBytes;
		}
		
		public boolean hasUnprocessedFreelistEntries() {
			return false;
		}
		
		public void processFreelistEntries() {
			
		}
		
		@Override
		public int capacity() {
			return capacity;
		}
		
		@Override
		public boolean isDirect() {
			return delegate.isDirect();
		}
		
		@Override
		public long memoryAddress() {
			return delegate._memoryAddress();
		}
	}
	
	private static final class IntStack {
		private final int[] stack;
		private int top;
		
		IntStack(int[] initialValues) {
			stack = initialValues;
			top = initialValues.length - 1;
		}
		
		public boolean isEmpty() {
			return top == -1;
		}
		public int pop() {
			final int last = stack[top];
			top--;
			return last;
		}
		
		public void push(int value) {
			stack[top + 1] = value;
			top++;
		}
		
		public int size() {
			return top + 1;
		}
	}
	
	private static final class SizeClassedChunk extends Chunk {
		private static final int FREE_LIST_EMPTY = -1;
		private static final int AVAILABLE = -1;
		private static final int DEALLOCATED = Integer.MIN_VALUE;
		private static final AtomicIntegerFieldUpdater<SizeClassedChunk> STATE = 
				AtomicIntegerFieldUpdater.newUpdater(SizeClassedChunk.class, "state");
		private volatile int state;
		private final int segments;
		private final int segmentSize;
		private final MpscIntQueue externalFreeList;
		private final IntStack localFreeList;
		private Thread ownerThread;
		
		SizeClassedChunk(AbstractByteBuf delegate, Magazine magazine,
				SizeClassChunkController controller) {
			super(delegate, magazine, true);
			segmentSize = controller.segmentSize;
			segments = controller.chunkSize / segmentSize;
			STATE.lazySet(this, AVAILABLE);
			ownerThread = magazine.group.ownerThread;
			if (ownerThread == null) {
				externalFreeList = controller.createFreeList();
				localFreeList = null;
			} else {
				externalFreeList = controller.createEmptyFreeList();
				localFreeList = controller.createLocalFreeList();
			}
		}
	}
	
	static final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {
		private final EnhancedHandle<AdaptiveByteBuf> handle;
		
		private int startIndex;
		private AbstractByteBuf rootParent;
		Chunk chunk;
		private int length;
		private int maxFastCapacity;
		private ByteBuffer tmpNioBuf;
		private boolean hasArray;
		private boolean hasMemoryAddress;
		
		AdaptiveByteBuf(EnhancedHandle<AdaptiveByteBuf> recyclerHandle) {
			super(0);
			handle = ObjectUtil.checkNotNull(recyclerHandle, "recyclerHandle");
		}
		
		void init(AbstractByteBuf unwrapped, Chunk wrapped, int readerIndex, int writerIndex,
				int startIndex, int size, int capacity, int maxCapacity) {
			this.startIndex = startIndex;
			chunk = wrapped;
			length = size;
			maxFastCapacity = capacity;
			maxCapacity(maxCapacity);
			setIndex0(readerIndex, writerIndex);
			hasArray = unwrapped.hasArray();
			hasMemoryAddress = unwrapped.hasMemoryAddress();
			rootParent = unwrapped;
			tmpNioBuf = null;
			
			if (PlatformDependent.isJfrEnabled() && AllocateBufferEvent.isEventEnabled()) {
				AllocateBufferEvent event = new AllocateBufferEvent();
				if (event.shouldCommit()) {
					event.fill(this, AdaptiveByteBufAllocator.class);
					event.chunkPooled = wrapped.pooled;
					Magazine m = wrapped.magazine;
					event.chunkThreadLocal = m != null && m.allocationLock == null;
					event.commit();
				}
			}
		}
		
		private AbstractByteBuf rootParent() {
			final AbstractByteBuf rootParent = this.rootParent;
			if (rootParent != null) {
				return rootParent;
			}
			throw new IllegalReferenceCountException();
		}
		
		@Override
		public int capacity() {
			return length;
		}
		
		@Override
		public int maxFastWritableBytes() {
			return Math.min(maxFastCapacity, maxCapacity()) - writerIndex;
		}
		
		@Override
		public ByteBuf capacity(int newCapacity) {
			checkNewCapacity(newCapacity);
			if (length <= newCapacity && newCapacity <= maxFastCapacity) {
				length = newCapacity;
				return this;
			}
			if (newCapacity < capacity()) {
				length = newCapacity;
				trimIndiciesToCapacity(newCapacity);
				return this;
			}
			
			if (PlatformDependent.isJfrEnabled() && ReallocateBufferEvent.isEventEnabled()) {
				ReallocateBufferEvent event = new ReallocateBufferEvent();
				if (event.shouldCommit()) {
					event.fill(this, AdaptiveByteBufAllocator.class);
					event.newCapacity = newCapacity;
					event.commit();
				}
			}
			
			Chunk chunk = this.chunk;
			AdaptivePoolingAllocator allocator = chunk.allocator;
			int readerIndex = this.readerIndex;
			int writerIndex = this.writerIndex;
			int baseOldRootIndex = startIndex;
			int oldLength = length;
			int oldCapacity = maxFastCapacity;
			AbstractByteBuf oldRoot = rootParent();
			allocator.reallocate(newCapacity, maxCapacity(), this);
			oldRoot.getBytes(baseOldRootIndex, this, 0, oldLength);
			chunk.releaseSegment(baseOldRootIndex, oldCapacity);
			assert oldCapacity < maxFastCapacity && newCapacity <= maxFastCapacity :
				"Capacity increase failed";
			this.readerIndex = readerIndex;
			this.writerIndex = writerIndex;
			return this;
		}
		
		@Override
        public ByteBufAllocator alloc() {
            return rootParent().alloc();
        }

        @Override
        public ByteOrder order() {
            return rootParent().order();
        }

        @Override
        public ByteBuf unwrap() {
            return null;
        }

        @Override
        public boolean isDirect() {
            return rootParent().isDirect();
        }

        @Override
        public int arrayOffset() {
            return idx(rootParent().arrayOffset());
        }

        @Override
        public boolean hasMemoryAddress() {
            return hasMemoryAddress;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return _memoryAddress();
        }

        @Override
        long _memoryAddress() {
            AbstractByteBuf root = rootParent;
            return root != null ? root._memoryAddress() + startIndex : 0L;
        }

        @Override
        boolean _isDirect() {
            AbstractByteBuf root = rootParent;
            return root != null && root.isDirect();
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffer(idx(index), length);
        }
        
        @Override
        public ByteBuffer internalNioBuffer(int index, int length) {
            checkIndex(index, length);
            return (ByteBuffer) internalNioBuffer().position(index).limit(index + length);
        }

        private ByteBuffer internalNioBuffer() {
            if (tmpNioBuf == null) {
                tmpNioBuf = rootParent().nioBuffer(startIndex, maxFastCapacity);
            }
            return (ByteBuffer) tmpNioBuf.clear();
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffers(idx(index), length);
        }

        @Override
        public boolean hasArray() {
            return hasArray;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            return rootParent().array();
        }

        @Override
        public ByteBuf copy(int index, int length) {
            checkIndex(index, length);
            return rootParent().copy(idx(index), length);
        }

        @Override
        public int nioBufferCount() {
            return rootParent().nioBufferCount();
        }

        @Override
        protected byte _getByte(int index) {
            return rootParent()._getByte(idx(index));
        }

        @Override
        protected short _getShort(int index) {
            return rootParent()._getShort(idx(index));
        }

        @Override
        protected short _getShortLE(int index) {
            return rootParent()._getShortLE(idx(index));
        }

        @Override
        protected int _getUnsignedMedium(int index) {
            return rootParent()._getUnsignedMedium(idx(index));
        }

        @Override
        protected int _getUnsignedMediumLE(int index) {
            return rootParent()._getUnsignedMediumLE(idx(index));
        }

        @Override
        protected int _getInt(int index) {
            return rootParent()._getInt(idx(index));
        }

        @Override
        protected int _getIntLE(int index) {
            return rootParent()._getIntLE(idx(index));
        }

        @Override
        protected long _getLong(int index) {
            return rootParent()._getLong(idx(index));
        }

        @Override
        protected long _getLongLE(int index) {
            return rootParent()._getLongLE(idx(index));
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuffer dst) {
            checkIndex(index, dst.remaining());
            rootParent().getBytes(idx(index), dst);
            return this;
        }

        @Override
        protected void _setByte(int index, int value) {
            rootParent()._setByte(idx(index), value);
        }

        @Override
        protected void _setShort(int index, int value) {
            rootParent()._setShort(idx(index), value);
        }

        @Override
        protected void _setShortLE(int index, int value) {
            rootParent()._setShortLE(idx(index), value);
        }

        @Override
        protected void _setMedium(int index, int value) {
            rootParent()._setMedium(idx(index), value);
        }

        @Override
        protected void _setMediumLE(int index, int value) {
            rootParent()._setMediumLE(idx(index), value);
        }

        @Override
        protected void _setInt(int index, int value) {
            rootParent()._setInt(idx(index), value);
        }

        @Override
        protected void _setIntLE(int index, int value) {
            rootParent()._setIntLE(idx(index), value);
        }

        @Override
        protected void _setLong(int index, long value) {
            rootParent()._setLong(idx(index), value);
        }

        @Override
        protected void _setLongLE(int index, long value) {
            rootParent().setLongLE(idx(index), value);
        }

        @Override
        public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
            checkIndex(index, length);
            if (tmpNioBuf == null && PlatformDependent.javaVersion() >= 13) {
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), src, srcIndex, length);
            } else {
                ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
                tmp.put(src, srcIndex, length);
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
            checkIndex(index, length);
            if (src instanceof AdaptiveByteBuf && PlatformDependent.javaVersion() >= 16) {
                AdaptiveByteBuf srcBuf = (AdaptiveByteBuf) src;
                srcBuf.checkIndex(srcIndex, length);
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                ByteBuffer srcBuffer = srcBuf.rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), srcBuffer, srcBuf.idx(srcIndex), length);
            } else {
                ByteBuffer tmp = internalNioBuffer();
                tmp.position(index);
                tmp.put(src.nioBuffer(srcIndex, length));
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuffer src) {
            int length = src.remaining();
            checkIndex(index, length);
            ByteBuffer tmp = internalNioBuffer();
            if (PlatformDependent.javaVersion() >= 16) {
                int offset = src.position();
                PlatformDependent.absolutePut(tmp, index, src, offset, length);
                src.position(offset + length);
            } else {
                tmp.position(index);
                tmp.put(src);
            }
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, OutputStream out, int length)
                throws IOException {
            checkIndex(index, length);
            if (length != 0) {
                ByteBuffer tmp = internalNioBuffer();
                ByteBufUtil.readBytes(alloc(), tmp.hasArray() ? tmp : tmp.duplicate(), index, length, out);
            }
            return this;
        }

        @Override
        public int getBytes(int index, GatheringByteChannel out, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf);
        }

        @Override
        public int getBytes(int index, FileChannel out, long position, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf, position);
        }

        @Override
        public int setBytes(int index, InputStream in, int length)
                throws IOException {
            checkIndex(index, length);
            final AbstractByteBuf rootParent = rootParent();
            if (rootParent.hasArray()) {
                return rootParent.setBytes(idx(index), in, length);
            }
            byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
            int readBytes = in.read(tmp, 0, length);
            if (readBytes <= 0) {
                return readBytes;
            }
            setBytes(index, tmp, 0, readBytes);
            return readBytes;
        }

        @Override
        public int setBytes(int index, ScatteringByteChannel in, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length));
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setBytes(int index, FileChannel in, long position, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length), position);
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setCharSequence(int index, CharSequence sequence, Charset charset) {
            return setCharSequence0(index, sequence, charset, false);
        }

        private int setCharSequence0(int index, CharSequence sequence, Charset charset, boolean expand) {
            if (charset.equals(CharsetUtil.UTF_8)) {
                int length = ByteBufUtil.utf8MaxBytes(sequence);
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeUtf8(this, index, length, sequence, sequence.length());
            }
            if (charset.equals(CharsetUtil.US_ASCII) || charset.equals(CharsetUtil.ISO_8859_1)) {
                int length = sequence.length();
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeAscii(this, index, sequence, length);
            }
            byte[] bytes = sequence.toString().getBytes(charset);
            if (expand) {
                ensureWritable0(bytes.length);
                // setBytes(...) will take care of checking the indices.
            }
            setBytes(index, bytes);
            return bytes.length;
        }

        @Override
        public int writeCharSequence(CharSequence sequence, Charset charset) {
            int written = setCharSequence0(writerIndex, sequence, charset, true);
            writerIndex += written;
            return written;
        }

        @Override
        public int forEachByte(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByte(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public int forEachByteDesc(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByteDesc(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public ByteBuf setZero(int index, int length) {
            checkIndex(index, length);
            rootParent().setZero(idx(index), length);
            return this;
        }

        @Override
        public ByteBuf writeZero(int length) {
            ensureWritable(length);
            rootParent().setZero(idx(writerIndex), length);
            writerIndex += length;
            return this;
        }

        private int forEachResult(int ret) {
            if (ret < startIndex) {
                return -1;
            }
            return ret - startIndex;
        }

        @Override
        public boolean isContiguous() {
            return rootParent().isContiguous();
        }

        private int idx(int index) {
            return index + startIndex;
        }

        @Override
        protected void deallocate() {
            if (PlatformDependent.isJfrEnabled() && FreeBufferEvent.isEventEnabled()) {
                FreeBufferEvent event = new FreeBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.commit();
                }
            }

            if (chunk != null) {
                chunk.releaseSegment(startIndex, maxFastCapacity);
            }
            tmpNioBuf = null;
            chunk = null;
            rootParent = null;
            handle.unguardedRecycle(this);
        }
	}
	
	interface ChunkAllocator {
		AbstractByteBuf allocate(int initialCapacity, int maxCapacity);
	}
}
