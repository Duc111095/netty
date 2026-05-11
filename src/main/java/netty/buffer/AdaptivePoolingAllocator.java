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
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.IntConsumer;

import netty.common.util.ByteProcessor;
import netty.common.util.CharsetUtil;
import netty.common.util.IllegalReferenceCountException;
import netty.common.util.NettyRuntime;
import netty.common.util.Recycler;
import netty.common.util.Recycler.EnhancedHandle;
import netty.common.util.concurrent.ConcurrentSkipListIntObjMultimap;
import netty.common.util.concurrent.ConcurrentSkipListIntObjMultimap.IntEntry;
import netty.common.util.concurrent.FastThreadLocal;
import netty.common.util.concurrent.FastThreadLocalThread;
import netty.common.util.concurrent.MpscIntQueue;
import netty.common.util.internal.MathUtil;
import netty.common.util.internal.ObjectUtil;
import netty.common.util.internal.PlatformDependent;
import netty.common.util.internal.RefCnt;
import netty.common.util.internal.SystemPropertyUtil;
import netty.common.util.internal.ThreadExecutorMap;
import netty.common.util.internal.UnstableApi;

@UnstableApi
final class AdaptivePoolingAllocator {
	private static final int LOW_MEM_THRESHOLD = 512 * 1024 * 1024;
	private static final boolean IS_LOW_MEM = Runtime.getRuntime().maxMemory() <= LOW_MEM_THRESHOLD;
	
	private static final boolean DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM = SystemPropertyUtil.getBoolean(
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
	private final MagazineGroup[] sizeClassedMagazineGroups;
	private final MagazineGroup largeBufferMagazineGroup;
	private final FastThreadLocal<MagazineGroup[]> threadLocalGroup;
	
	AdaptivePoolingAllocator(ChunkAllocator chunkAllocator, boolean useCacheForNonEventLoopThreads) {
		this.chunkAllocator = ObjectUtil.checkNotNull(chunkAllocator, "chunkAllocator");
		chunkRegistry = new ChunkRegistry();
		sizeClassedMagazineGroups = createMagazineGroupSizeClasses(this, false);
		largeBufferMagazineGroup = new MagazineGroup(
				this, chunkAllocator, new BuddyChunkManagementStrategy(), false);
		
		boolean disableThreadLocalGroups = IS_LOW_MEM && DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM;
		threadLocalGroup = disableThreadLocalGroups ? null : new FastThreadLocal<MagazineGroup[]>() {
			@Override
			protected MagazineGroup[] initialValue() {
				if (useCacheForNonEventLoopThreads || ThreadExecutorMap.currentExecutor() != null) {
					return createMagazineGroupSizeClasses(AdaptivePoolingAllocator.this, true);
				}
				return null;
			}
			
			@Override
			protected void onRemoval(final MagazineGroup[] groups) throws Exception {
				if (groups != null) {
					for (MagazineGroup group : groups) {
						group.free();
					}
				}
			}
		};
	}
	
	private static MagazineGroup[] createMagazineGroupSizeClasses(
			AdaptivePoolingAllocator allocator, boolean isThreadLocal) {
		MagazineGroup[] groups = new MagazineGroup[SIZE_CLASSES.length];
		for (int i = 0;  i < SIZE_CLASSES.length; i++) {
			int segmentSize = SIZE_CLASSES[i];
			groups[i] = new MagazineGroup(allocator, allocator.chunkAllocator,
					new SizeClassChunkManagementStrategy(segmentSize), isThreadLocal);
		}
		return groups;
	}
	
	private static Queue<SizeClassedChunk> createSharedChunkQueue() {
		return PlatformDependent.newFixedMpmcQueue(CHUNK_REUSE_QUEUE);
	}
	
	ByteBuf allocate(int size, int maxCapacity) {
		return allocate(size, maxCapacity, Thread.currentThread(), null);
	}
	
	private AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
		AdaptiveByteBuf allocated = null;
        if (size <= MAX_POOLED_BUF_SIZE) {
            final int index = sizeClassIndexOf(size);
            MagazineGroup[] magazineGroups;
            if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals() ||
                    IS_LOW_MEM ||
                    (magazineGroups = threadLocalGroup.get()) == null) {
                magazineGroups =  sizeClassedMagazineGroups;
            }
            if (index < magazineGroups.length) {
                allocated = magazineGroups[index].allocate(size, maxCapacity, currentThread, buf);
            } else if (!IS_LOW_MEM) {
                allocated = largeBufferMagazineGroup.allocate(size, maxCapacity, currentThread, buf);
            }
        }
        if (allocated == null) {
            allocated = allocateFallback(size, maxCapacity, currentThread, buf);
        }
        return allocated;
	}
	
	private static int sizeIndexOf(final int size) {
		return size + 31 >> 5;
	}
	
	static int sizeClassIndexOf(int size) {
		int sizeIndex = sizeIndexOf(size);
		if (sizeIndex < SIZE_INDEXES.length) {
			return SIZE_INDEXES[sizeIndex];
		}
		
		return SIZE_CLASSES_COUNT;
	}
	
	static int[] getSizeClasses() {
		return SIZE_CLASSES.clone();
	}
	
	private AdaptiveByteBuf allocateFallback(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
		Magazine magazine;
		if (buf != null) {
			Chunk chunk = buf.chunk;
			if (chunk == null || chunk == Magazine.MAGAZINE_FREED || (magazine = chunk.currentMagazine()) == null) {
				magazine = getFallbackMagazine(currentThread);
			}
		} else {
			magazine = getFallbackMagazine(currentThread);
			buf = magazine.newBuffer();
		}
		AbstractByteBuf innerChunk = chunkAllocator.allocate(size, maxCapacity);
		Chunk chunk = new Chunk(innerChunk, magazine, false);
		chunkRegistry.add(chunk);
		try {
			boolean success = chunk.readInitInto(buf, size, size, maxCapacity);
			assert success : "Failed to initialize ByteBuf with dedicated chunk";
		} finally {
			chunk.release();
		}
		return buf;
	}
	
	private Magazine getFallbackMagazine(Thread currentThread) {
		Magazine[] mags = largeBufferMagazineGroup.magazines;
		return mags[(int) currentThread.getId() & mags.length - 1]; 
	}
	
	void reallocate(int size, int maxCapacity, AdaptiveByteBuf into) {
		AdaptiveByteBuf result = allocate(size, maxCapacity, Thread.currentThread(), into);
		assert result == into : "Re-allocation created separate buffer instance";
	}
	
	long usedMemory() {
		return chunkRegistry.totalCapacity();
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		try {
			free();
		} finally {
			super.finalize();
		}
	}
	
	private void free() {
		largeBufferMagazineGroup.free();
	}
	
	
	private static final class MagazineGroup {
		private final AdaptivePoolingAllocator allocator;
		private final ChunkAllocator chunkAllocator;
		private final ChunkManagementStrategy chunkManagementStrategy;
		private final ChunkCache chunkCache;
		private final StampedLock magazineExpandLock;
		private final Magazine threadLocalMagazine;
		private Thread ownerThread;
		private volatile Magazine[] magazines;
		private volatile boolean freed;
		
		MagazineGroup(AdaptivePoolingAllocator allocator,
				ChunkAllocator chunkAllocator,
				ChunkManagementStrategy chunkManagementStrategy,
				boolean isThreadLocal) {
			this.allocator = allocator;
			this.chunkAllocator = chunkAllocator;
			this.chunkManagementStrategy = chunkManagementStrategy;
			chunkCache = chunkManagementStrategy.createChunkCache(isThreadLocal);
			if (isThreadLocal) {
				ownerThread = Thread.currentThread();
				magazineExpandLock = null;
				threadLocalMagazine = new Magazine(this, false, chunkManagementStrategy.createController(this));
			} else {
				ownerThread = null;
				magazineExpandLock = new StampedLock();
				threadLocalMagazine = null;
				Magazine[] mags = new Magazine[INITIAL_MAGAZINES];
				for (int i = 0; i < mags.length; i++) {
					mags[i] = new Magazine(this, true, chunkManagementStrategy.createController(this));
				}
				magazines = mags;
			}
		}
		
		public AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
			boolean reallocate = buf != null;
			
			Magazine tlMag = threadLocalMagazine;
			if (tlMag != null) {
				if (buf == null) {
					buf = tlMag.newBuffer();
				}
				boolean allocated = tlMag.tryAllocate(size, maxCapacity, buf, reallocate);
				assert allocated : "Allocation of threadLocalMagazine must always succeed";
				return buf;
			}
			
			long threadId = currentThread.getId();
			Magazine[] mags;
			int expansions = 0;
			do {
				mags = magazines;
				int mask = mags.length - 1;
				int index = (int) (threadId & mask);
				for (int i = 0, m = mags.length << 1; i < m; i++) {
					Magazine mag = mags[index + i & mask];
					if (buf == null) {
						buf = mag.newBuffer();
					}
					if (mag.tryAllocate(size, maxCapacity, buf, reallocate)) {
						return buf;
					}
				}
				expansions++;
			} while (expansions <= EXPANSION_ATTEMPTS && tryExpandMagazines(mags.length));
			
			if (!reallocate && buf != null) {
				buf.release();
			}
			return null;
		}
		
		private boolean tryExpandMagazines(int currentLength) {
			if (currentLength >= MAX_STRIPES) {
				return true;
			}
			final Magazine[] mags;
			long writeLock = magazineExpandLock.tryWriteLock();
			if (writeLock != 0) {
				try {
					mags = magazines;
					if (mags.length >= MAX_STRIPES || mags.length > currentLength || freed) {
						return true;
					}
					Magazine[] expanded = new Magazine[mags.length * 2];
					for (int i = 0, l = expanded.length; i < l; i++) {
						expanded[i] = new Magazine(this, true, chunkManagementStrategy.createController(this));
					}
					magazines = expanded;
				} finally {
					magazineExpandLock.unlock(writeLock);
				}
				for (Magazine magazine : mags) {
					magazine.free();
				}
			}
			return true;
		}
		
		Chunk pollChunk(int size) {
            return chunkCache.pollChunk(size);
        }

        boolean offerChunk(Chunk chunk) {
            if (freed) {
                return false;
            }

            if (chunk.hasUnprocessedFreelistEntries()) {
                chunk.processFreelistEntries();
            }
            boolean isAdded = chunkCache.offerChunk(chunk);

            if (freed && isAdded) {
                // Help to free the reuse queue.
                freeChunkReuseQueue(ownerThread);
            }
            return isAdded;
        }

        private void free() {
            freed = true;
            Thread ownerThread = this.ownerThread;
            if (threadLocalMagazine != null) {
                this.ownerThread = null;
                threadLocalMagazine.free();
            } else {
                long stamp = magazineExpandLock.writeLock();
                try {
                    Magazine[] mags = magazines;
                    for (Magazine magazine : mags) {
                        magazine.free();
                    }
                } finally {
                    magazineExpandLock.unlockWrite(stamp);
                }
            }
            freeChunkReuseQueue(ownerThread);
        }

        private void freeChunkReuseQueue(Thread ownerThread) {
            Chunk chunk;
            while ((chunk = chunkCache.pollChunk(0)) != null) {
                if (ownerThread != null && chunk instanceof SizeClassedChunk) {
                    SizeClassedChunk threadLocalChunk = (SizeClassedChunk) chunk;
                    assert ownerThread == threadLocalChunk.ownerThread;
                    // no release segment can ever happen from the owner Thread since it's not running anymore
                    // This is required to let the ownerThread to be GC'ed despite there are AdaptiveByteBuf
                    // that reference some thread local chunk
                    threadLocalChunk.ownerThread = null;
                }
                chunk.markToDeallocate();
            }
        }
	}
	
	private interface ChunkCache {
		Chunk pollChunk(int size);
		boolean offerChunk(Chunk chunk);
	}
	
	private static final class ConcurrentQueueChunkCache implements ChunkCache {
		private final Queue<SizeClassedChunk> queue;
		
		private ConcurrentQueueChunkCache() {
			queue = createSharedChunkQueue();
		}
		
		@Override
		public SizeClassedChunk pollChunk(int size) {
			Queue<SizeClassedChunk> queue = this.queue;
			for (int i = 0; i < CHUNK_REUSE_QUEUE; i++) {
				SizeClassedChunk chunk = queue.poll();
				if (chunk == null) {
					return null;
				}
				if (chunk.hasRemainingCapacity()) {
					return chunk;
				}
				queue.offer(chunk);
			}
			return null;
		}
		
		@Override
		public boolean offerChunk(Chunk chunk) {
			return queue.offer((SizeClassedChunk) chunk);
		}
	}
	
	private static final class ConcurrentSkipListChunkCache implements ChunkCache {
        private final ConcurrentSkipListIntObjMultimap<Chunk> chunks;

        private ConcurrentSkipListChunkCache() {
            chunks = new ConcurrentSkipListIntObjMultimap<>(-1);
        }

        @Override
        public Chunk pollChunk(int size) {
            if (chunks.isEmpty()) {
                return null;
            }
            IntEntry<Chunk> entry = chunks.pollCeilingEntry(size);
            if (entry != null) {
                Chunk chunk = entry.getValue();
                if (chunk.hasUnprocessedFreelistEntries()) {
                    chunk.processFreelistEntries();
                }
                return chunk;
            }

            Chunk bestChunk = null;
            int bestRemainingCapacity = 0;
            Iterator<IntEntry<Chunk>> itr = chunks.iterator();
            while (itr.hasNext()) {
                entry = itr.next();
                final Chunk chunk;
                if (entry != null && (chunk = entry.getValue()).hasUnprocessedFreelistEntries()) {
                    if (!chunks.remove(entry.getKey(), entry.getValue())) {
                        continue;
                    }
                    chunk.processFreelistEntries();
                    int remainingCapacity = chunk.remainingCapacity();
                    if (remainingCapacity >= size &&
                            (bestChunk == null || remainingCapacity > bestRemainingCapacity)) {
                        if (bestChunk != null) {
                            chunks.put(bestRemainingCapacity, bestChunk);
                        }
                        bestChunk = chunk;
                        bestRemainingCapacity = remainingCapacity;
                    } else {
                        chunks.put(remainingCapacity, chunk);
                    }
                }
            }

            return bestChunk;
        }

        @Override
        public boolean offerChunk(Chunk chunk) {
            chunks.put(chunk.remainingCapacity(), chunk);

            int size = chunks.size();
            while (size > CHUNK_REUSE_QUEUE) {
                // Deallocate the chunk with the fewest incoming references.
                int key = -1;
                Chunk toDeallocate = null;
                for (IntEntry<Chunk> entry : chunks) {
                    Chunk candidate = entry.getValue();
                    if (candidate != null) {
                        if (toDeallocate == null) {
                            toDeallocate = candidate;
                            key = entry.getKey();
                        } else {
                            int candidateRefCnt = RefCnt.refCnt(candidate.refCnt);
                            int toDeallocateRefCnt = RefCnt.refCnt(toDeallocate.refCnt);
                            if (candidateRefCnt < toDeallocateRefCnt ||
                                    candidateRefCnt == toDeallocateRefCnt &&
                                            candidate.capacity() < toDeallocate.capacity()) {
                                toDeallocate = candidate;
                                key = entry.getKey();
                            }
                        }
                    }
                }
                if (toDeallocate == null) {
                    break;
                }
                if (chunks.remove(key, toDeallocate)) {
                    toDeallocate.markToDeallocate();
                }
                size = chunks.size();
            }
            return true;
        }
    }
	
	private interface ChunkManagementStrategy {
		ChunkController createController(MagazineGroup group);
		
		ChunkCache createChunkCache(boolean isThreadLocal);
	}
	
	private interface ChunkController {
		int computeBufferCapacity(int requestedSize, int maxCapacity, boolean isReallocation);
		
		Chunk newChunkAllocation(int promptingSize, Magazine magazine);
	}
	
	private static final class SizeClassChunkManagementStrategy implements ChunkManagementStrategy {
		private static final int MIN_SEGMENTS_PER_CHUNK = 32;
		private final int segmentSize;
		private final int chunkSize;
		
		private SizeClassChunkManagementStrategy(int segmentSize) {
			this.segmentSize = ObjectUtil.checkPositive(segmentSize, "segmentSize");
			chunkSize = Math.max(MIN_CHUNK_SIZE, segmentSize * MIN_SEGMENTS_PER_CHUNK);
		}
		
		@Override
		public ChunkController createController(MagazineGroup group) {
			return new SizeClassChunkController(group, segmentSize, chunkSize);
		}
		
		@Override
		public ChunkCache createChunkCache(boolean isThreadLocal) {
			return new ConcurrentQueueChunkCache();
		}
	}
	
	private static final class SizeClassChunkController implements ChunkController {

        private final ChunkAllocator chunkAllocator;
        private final int segmentSize;
        private final int chunkSize;
        private final ChunkRegistry chunkRegistry;

        private SizeClassChunkController(MagazineGroup group, int segmentSize, int chunkSize) {
            chunkAllocator = group.chunkAllocator;
            this.segmentSize = segmentSize;
            this.chunkSize = chunkSize;
            chunkRegistry = group.allocator.chunkRegistry;
        }

        private MpscIntQueue createEmptyFreeList() {
            return MpscIntQueue.create(chunkSize / segmentSize, SizeClassedChunk.FREE_LIST_EMPTY);
        }

        private MpscIntQueue createFreeList() {
            final int segmentsCount = chunkSize / segmentSize;
            final MpscIntQueue freeList = MpscIntQueue.create(segmentsCount, SizeClassedChunk.FREE_LIST_EMPTY);
            int segmentOffset = 0;
            for (int i = 0; i < segmentsCount; i++) {
                freeList.offer(segmentOffset);
                segmentOffset += segmentSize;
            }
            return freeList;
        }

        private IntStack createLocalFreeList() {
            final int segmentsCount = chunkSize / segmentSize;
            int segmentOffset = chunkSize;
            int[] offsets = new int[segmentsCount];
            for (int i = 0; i < segmentsCount; i++) {
                segmentOffset -= segmentSize;
                offsets[i] = segmentOffset;
            }
            return new IntStack(offsets);
        }

        @Override
        public int computeBufferCapacity(
                int requestedSize, int maxCapacity, boolean isReallocation) {
            return Math.min(segmentSize, maxCapacity);
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            AbstractByteBuf chunkBuffer = chunkAllocator.allocate(chunkSize, chunkSize);
            assert chunkBuffer.capacity() == chunkSize;
            SizeClassedChunk chunk = new SizeClassedChunk(chunkBuffer, magazine, this);
            chunkRegistry.add(chunk);
            return chunk;
        }
    }

    private static final class BuddyChunkManagementStrategy implements ChunkManagementStrategy {
        private final AtomicInteger maxChunkSize = new AtomicInteger();

        @Override
        public ChunkController createController(MagazineGroup group) {
            return new BuddyChunkController(group, maxChunkSize);
        }

        @Override
        public ChunkCache createChunkCache(boolean isThreadLocal) {
            return new ConcurrentSkipListChunkCache();
        }
    }

    private static final class BuddyChunkController implements ChunkController {
        private final ChunkAllocator chunkAllocator;
        private final ChunkRegistry chunkRegistry;
        private final AtomicInteger maxChunkSize;

        BuddyChunkController(MagazineGroup group, AtomicInteger maxChunkSize) {
            chunkAllocator = group.chunkAllocator;
            chunkRegistry = group.allocator.chunkRegistry;
            this.maxChunkSize = maxChunkSize;
        }

        @Override
        public int computeBufferCapacity(int requestedSize, int maxCapacity, boolean isReallocation) {
            return MathUtil.safeFindNextPositivePowerOfTwo(requestedSize);
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            int maxChunkSize = this.maxChunkSize.get();
            int proposedChunkSize = MathUtil.safeFindNextPositivePowerOfTwo(BUFS_PER_CHUNK * promptingSize);
            int chunkSize = Math.min(MAX_CHUNK_SIZE, Math.max(maxChunkSize, proposedChunkSize));
            if (chunkSize > maxChunkSize) {
                // Update our stored max chunk size. It's fine that this is racy.
                this.maxChunkSize.set(chunkSize);
            }
            BuddyChunk chunk = new BuddyChunk(chunkAllocator.allocate(chunkSize, chunkSize), magazine);
            chunkRegistry.add(chunk);
            return chunk;
        }
    }

	
	private static final class Magazine {
		private static final AtomicReferenceFieldUpdater<Magazine, Chunk> NEXT_IN_LINE;
		static {
			NEXT_IN_LINE = AtomicReferenceFieldUpdater.newUpdater(Magazine.class, Chunk.class, "nextInLine");
		}
		private static final Chunk MAGAZINE_FREED = new Chunk();
		
		private static final class AdaptiveRecycler extends Recycler<AdaptiveByteBuf> {
			private AdaptiveRecycler(boolean unguarded) {
				super(unguarded);
			}
			
			private AdaptiveRecycler(int maxCapacity, boolean unguarded) {
				super(maxCapacity, unguarded);
			}
			
			@Override
			protected AdaptiveByteBuf newObject(final Handle<AdaptiveByteBuf> handle) {
				return new AdaptiveByteBuf((EnhancedHandle<AdaptiveByteBuf>) handle);
			}
			
			public static AdaptiveRecycler threadLocal() {
				return new AdaptiveRecycler(true);
			}
			
			public static AdaptiveRecycler sharedWith(int maxCapacity) {
				return new AdaptiveRecycler(maxCapacity, true);
			}
		}
		
		private static final AdaptiveRecycler EVENT_LOOP_LOCAL_BUFFER_POOL = AdaptiveRecycler.threadLocal();
		
		private Chunk current;
		@SuppressWarnings("unused")
		private volatile Chunk nextInLine;
		private final MagazineGroup group;
		private final ChunkController chunkController;
		private final StampedLock allocationLock;
		private final AdaptiveRecycler recycler;
		
		Magazine(MagazineGroup group, boolean shareable, ChunkController chunkController) {
			this.group = group;
			this.chunkController = chunkController;
			if (shareable) {
				allocationLock = new StampedLock();
				recycler = AdaptiveRecycler.sharedWith(MAGAZINE_BUFFER_QUEUE_CAPACITY);
			} else {
				allocationLock = null;
				recycler = null;
			}
		}
		
		public boolean tryAllocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
			if (allocationLock == null) {
				return allocate(size, maxCapacity, buf, reallocate);
			}
			long writeLock = allocationLock.tryWriteLock();
			if (writeLock != 0) {
				try {
					return allocate(size, maxCapacity, buf, reallocate);
				} finally {
					allocationLock.unlockWrite(writeLock);
				}
			}
			return allocateWithoutLock(size, maxCapacity, buf);
		}
		
		private boolean allocateWithoutLock(int size, int maxCapacity, AdaptiveByteBuf buf) {
			Chunk curr = NEXT_IN_LINE.getAndSet(this, null);
			if (curr == MAGAZINE_FREED) {
				restoreMagazineFreed();
				return false;
			}
			if (curr == null) {
				curr = group.pollChunk(size);
				if (curr == null) {
					return false;
				}
				curr.attachToMagazine(this);
			}
			boolean allocated = false;
			int remainingCapacity = curr.remainingCapacity();
			int startingCapacity = chunkController.computeBufferCapacity(
					size, maxCapacity, true);
			if (remainingCapacity >= size &&
					curr.readInitInto(buf, size, Math.min(remainingCapacity, startingCapacity), maxCapacity)) {
				allocated = true;
				remainingCapacity = curr.remainingCapacity();
			}
			try {
				if (remainingCapacity >= RETIRE_CAPACITY) {
					transferToNextInLineOrRelease(curr);
					curr = null;
				}
			} finally {
				if (curr != null) {
					curr.releaseFromMagazine();
				}
			}
			return allocated;
		}
		
		private boolean allocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
			int startingCapacity = chunkController.computeBufferCapacity(size, maxCapacity, reallocate);
			Chunk curr = current;
			if (curr != null) {
				boolean success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
				int remainingCapacity = curr.remainingCapacity();
				if (!success && remainingCapacity > 0) {
					current = null;
					transferToNextInLineOrRelease(curr);
				} else if (remainingCapacity == 0) {
					current = null;
					curr.releaseFromMagazine();
				}
				if (success) {
					return true;
				}
			}
			
			assert current == null;
			curr = NEXT_IN_LINE.getAndSet(this, null);
			if (curr != null) {
				if (curr == MAGAZINE_FREED) {
					restoreMagazineFreed();
					return false;
				}
				
				int remainingCapacity = curr.remainingCapacity();
				if (remainingCapacity > startingCapacity && 
						curr.readInitInto(buf, size, startingCapacity, maxCapacity)) {
					current = curr;
					return true;
				}
				
				try {
					if (remainingCapacity >= size) {
						return curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
					}
				} finally {
					curr.releaseFromMagazine();
				}
			}
			
			curr = group.pollChunk(size);
			if (curr == null) {
				curr = chunkController.newChunkAllocation(size, this);
			} else {
				curr.attachToMagazine(this);
				
				int remainingCapacity = curr.remainingCapacity();
				if (remainingCapacity == 0 || remainingCapacity < size) {
					if (remainingCapacity < RETIRE_CAPACITY) {
						curr.releaseFromMagazine();
					} else {
						transferToNextInLineOrRelease(curr);
					}
					curr = chunkController.newChunkAllocation(size, this);
				}
			}
			
			current = curr;
			boolean success;
			try {
				int remainingCapacity = curr.remainingCapacity();
				assert remainingCapacity >= size;
				if (remainingCapacity > startingCapacity) {
					success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
					curr = null;
				} else {
					success = curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
				}
			} finally {
				if (curr != null) {
					curr.releaseFromMagazine();
					current = null;
				}
			}
			return success;
		}
		
		private void restoreMagazineFreed() {
			Chunk next = NEXT_IN_LINE.getAndSet(this, MAGAZINE_FREED);
			if (next != null && next != MAGAZINE_FREED) {
				next.releaseFromMagazine();
			}
		}
		
		private void transferToNextInLineOrRelease(Chunk chunk) {
			if (NEXT_IN_LINE.compareAndSet(this, null, chunk)) {
				return;
			}
			
			Chunk nextChunk = NEXT_IN_LINE.get(this);
			if (nextChunk != null && nextChunk != MAGAZINE_FREED
					&& chunk.remainingCapacity() > nextChunk.remainingCapacity()) {
				if (NEXT_IN_LINE.compareAndSet(this, nextChunk, chunk)) {
					nextChunk.releaseFromMagazine();
					return;
				}
			}
			
			chunk.releaseFromMagazine();
		}
		
		void free() {
			restoreMagazineFreed();
			long stamp = allocationLock != null ? allocationLock.writeLock() : 0;
			try {
				if (current != null) {
					current.releaseFromMagazine();
					current = null;
				}
			} finally {
				if (allocationLock != null) {
					allocationLock.unlockWrite(stamp);
				}
			}
		}
		
		public AdaptiveByteBuf newBuffer() {
			AdaptiveRecycler recycler = this.recycler;
			AdaptiveByteBuf buf = recycler == null ? EVENT_LOOP_LOCAL_BUFFER_POOL.get() : recycler.get();
			buf.resetRefCnt();
			buf.discardMarks();
			return buf;
		}
		
		boolean offerToQueue(Chunk chunk) {
			return group.offerChunk(chunk);
		}
	}
	
	private static final class ChunkRegistry {
		private final LongAdder totalCapacity = new LongAdder();
		
		public long totalCapacity() {
			return totalCapacity.sum();
		}
		
		public void add(Chunk chunk) {
			totalCapacity.add(chunk.capacity());
		}
		
		public void remove(Chunk chunk) {
			totalCapacity.add(-chunk.capacity());
		}
	}
	
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
			assert this.magazine == null;
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
		
		@Override
		public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
			assert state == AVAILABLE;
			final int startIndex = nextAvailableSegmentOffset();
			if (startIndex == FREE_LIST_EMPTY) {
				return false;
			}
			allocatedBytes += segmentSize;
			try {
				buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
			} catch (Throwable t) {
				allocatedBytes -= segmentSize;
				releaseSegmentOffsetIntoFreeList(startIndex);
				throw t;
			}
			return true;
		}
		
		private int nextAvailableSegmentOffset() {
			final int startIndex;
			IntStack localFreeList = this.localFreeList;
			if (localFreeList != null) {
				assert Thread.currentThread() == ownerThread;
				if (localFreeList.isEmpty()) {
					startIndex = externalFreeList.poll();
				} else {
					startIndex = localFreeList.pop();
				}
			} else {
				startIndex = externalFreeList.poll();
			}
			return startIndex;
		}
		
		public boolean hasRemainingCapacity() {
			int remaining = super.remainingCapacity();
			if (remaining > 0) {
				return true;
			}
			if (localFreeList != null) {
				return !localFreeList.isEmpty();
			}
			return !externalFreeList.isEmpty();
		}
		
		@Override
		public int remainingCapacity() {
			int remaining = super.remainingCapacity();
			return remaining > segmentSize ? remaining : updateRemainingCapacity(remaining);
		}
		
		private int updateRemainingCapacity(int snapshotted) {
			int freeSegments = externalFreeList.size();
			IntStack localFreeList = this.localFreeList;
			if (localFreeList != null) {
				freeSegments += localFreeList.size();
			}
			int updated = freeSegments * segmentSize;
			if (updated != snapshotted) {
				allocatedBytes = capacity() - updated;
			}
			return updated;
		}
		
		private void releaseSegmentOffsetIntoFreeList(int startIndex) {
			IntStack localFreeList = this.localFreeList;
			if (localFreeList != null && Thread.currentThread() == ownerThread) {
				localFreeList.push(startIndex);
			} else {
				boolean segmentReturned = externalFreeList.offer(startIndex);
				assert segmentReturned : "Unable to return segment " + startIndex + " to free list";
			}
		}
		
		@Override
		void releaseSegment(int startIndex, int size) {
			IntStack localFreeList = this.localFreeList;
			if (localFreeList != null && Thread.currentThread() == ownerThread) {
				localFreeList.push(startIndex);
				int state = this.state;
				if (state != AVAILABLE) {
					updateStateOnLocalReleaseSegment(state, localFreeList);
				}
			} else {
				boolean segmentReturned = externalFreeList.offer(startIndex);
				assert segmentReturned;
				int state = this.state;
				if (state != AVAILABLE) {
					deallocateIfNeeded(state);
				}
			}
		}
		
		private void updateStateOnLocalReleaseSegment(int previousLocalSize, IntStack localFreeList) {
			int newLocalSize = localFreeList.size();
			boolean alwaysTrue = STATE.compareAndSet(this, previousLocalSize, newLocalSize);
			assert alwaysTrue : "this shouldn't happen unless double release in the local free list";
			deallocateIfNeeded(newLocalSize);
		}
		
		private void deallocateIfNeeded(int localSize) {
			int totalFreeSegments = localSize + externalFreeList.size();
			if (totalFreeSegments == segments && STATE.compareAndSet(this, localSize, DEALLOCATED)) {
				deallocate();
			}
		}
		
		@Override
		void markToDeallocate() {
			IntStack localFreeList = this.localFreeList;
			int localSize = localFreeList != null ? localFreeList.size() : 0;
			STATE.set(this, localSize);
			deallocateIfNeeded(localSize);
		}
	}
	
	private static final class BuddyChunk extends Chunk implements IntConsumer {
		private static final int MIN_BUDDY_SIZE = 32768;
		private static final byte IS_CLAIMED = (byte) (1 << 7);
		private static final byte HAS_CLAIMED_CHILDREN = 1 << 6;
		private static final byte SHIFT_MASK = ~(IS_CLAIMED | HAS_CLAIMED_CHILDREN);
		private static final int PACK_OFFSET_MASK = 0xFFFF;
		private static final int PACK_SIZE_SHIFT = Integer.SIZE - Integer.numberOfLeadingZeros(PACK_OFFSET_MASK);
		
		private final MpscIntQueue freeList;
		private final byte[] buddies;
		private final int freeListCapacity;
		
		BuddyChunk(AbstractByteBuf delegate, Magazine magazine) {
			super(delegate, magazine, true);
			freeListCapacity = delegate.capacity() / MIN_BUDDY_SIZE;
			int maxShift = Integer.numberOfTrailingZeros(freeListCapacity);
			assert maxShift <= 30;
			freeList = MpscIntQueue.create(freeListCapacity, -1);
			buddies = new byte[freeListCapacity << 1];
			
			int index = 1;
			int runLength = 1;
			int currentRun = 0;
			while (maxShift > 0) {
				buddies[index++] = (byte) maxShift;
				if (++currentRun == runLength) {
					currentRun = 0;
					runLength <<= 1;
					maxShift--;
				}
			}
		}
		
		@Override
		public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
			if (!freeList.isEmpty()) {
				freeList.drain(freeListCapacity, this);
			}
			int startIndex = chooseFirstFreeBuddy(1, startingCapacity, 0);
			if (startIndex == -1) {
				return false;
			}
			Chunk chunk = this;
			chunk.retain();
			try {
				buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
				allocatedBytes += startingCapacity;
				chunk = null;
			} finally {
				if (chunk != null) {
					unreserveMatchingBuddy(1, startingCapacity, startIndex, 0);
					chunk.release();
				}
			}
			return true;
		}
		
		@Override
		public void accept(int packed) {
			int size = unpackSize(packed);
			int offset = unpackOffset(packed);
			unreserveMatchingBuddy(1, size, offset, 0);
			allocatedBytes -= size;
		}
		
		private static int unpackSize(int packed) {
			return MIN_BUDDY_SIZE << (packed >> PACK_SIZE_SHIFT);
		}
		
		private static int unpackOffset(int packed) {
			return (packed & PACK_OFFSET_MASK) * MIN_BUDDY_SIZE;
		}
		
		@Override
		void releaseSegment(int startingIndex, int size) {
			int packedOffset = startingIndex / MIN_BUDDY_SIZE;
			int packedSize = Integer.numberOfTrailingZeros(size / MIN_BUDDY_SIZE) << PACK_SIZE_SHIFT;
			int packed = packedOffset | packedSize;
			freeList.offer(packed);
			release();
		}
		
		@Override
		public int remainingCapacity() {
			int capacityInFreeList = 0;
			if (!freeList.isEmpty()) {
				capacityInFreeList = freeList.weakPeekReduce(freeListCapacity, 0,
						(sum, entry) -> sum + unpackSize(entry));
			}
			return super.remainingCapacity() + capacityInFreeList;
		}
		
		@Override
		public boolean hasUnprocessedFreelistEntries() {
			return !freeList.isEmpty();
		}
		
		@Override
		public void processFreelistEntries() {
			freeList.drain(freeListCapacity, this);
		}
		
		private int chooseFirstFreeBuddy(int index, int size, int currOffset) {
			byte[] buddies = this.buddies;
			while (index < buddies.length) {
				byte buddy = buddies[index];
				int currValue = MIN_BUDDY_SIZE << (buddy & SHIFT_MASK);
				if (currValue < size || (buddy & IS_CLAIMED) == IS_CLAIMED) {
					return -1;
				}
				if (currValue == size && (buddy & HAS_CLAIMED_CHILDREN) == 0) {
					buddies[index] |= IS_CLAIMED;
					return currOffset;
				}
				int found = chooseFirstFreeBuddy(index << 1, size, currOffset);
				if (found != -1) {
					buddies[index] |= HAS_CLAIMED_CHILDREN;
					return found;
				}
				index = (index << 1) + 1;
				currOffset += currValue >> 1;
			}
			return -1;
		}
		
		private boolean unreserveMatchingBuddy(int index, int size, int offset, int currOffset) {
			byte[] buddies = this.buddies;
			if (buddies.length <= index) {
				return false;
			}
			byte buddy = buddies[index];
			int currSize = MIN_BUDDY_SIZE << (buddy & SHIFT_MASK);
			
			if (currSize == size) {
				if (currOffset == offset) {
					buddies[index] &= SHIFT_MASK;
					return false;
				}
				throw new IllegalStateException("The intended segment was not found at index " +
						index + ", for size " + size + " and offset " + offset);
			}
			
			boolean claims;
			int siblingIndex;
			if (offset < currOffset + (currSize >> 1)) {
				claims = unreserveMatchingBuddy(index << 1, size, offset, currOffset);
				siblingIndex = (index << 1) + 1;
			} else {
				claims = unreserveMatchingBuddy((index << 1) + 1, size, offset, currOffset + (currSize >> 1));
				siblingIndex = index << 1;
			}
			if (!claims) {
				byte sibling = buddies[siblingIndex];
				if ((sibling & SHIFT_MASK) == sibling) {
					buddies[index] &= SHIFT_MASK;
					return false;
				}
			}
			return true;
		}
		
		@Override
        public String toString() {
            int capacity = delegate.capacity();
            int remaining = capacity - allocatedBytes;
            return "BuddyChunk[capacity: " + capacity +
                    ", remaining: " + remaining +
                    ", free list: " + freeList.size() + ']';
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
