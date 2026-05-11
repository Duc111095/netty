package netty.buffer;

import static netty.buffer.PoolThreadCache.log2;

final class SizeClasses implements SizeClassesMetric {
	
	static final int LOG2_QUANTUM = 4;
	
	private static final int LOG2_SIZE_CLASS_GROUP = 2;
	private static final int LOG2_MAX_LOOKUP_SIZE = 12;
	
	private static final int LOG2GROUP_IDX = 1;
	private static final int LOG2DELTA_IDX = 2;
	private static final int NDELTA_IDX = 3;
	private static final int PAGESIZE_IDX = 4;
	private static final int SUBPAGE_IDX = 5;
	private static final int LOG2_DELTA_LOOKUP_IDX = 6;
	
	private static final byte no = 0, yes = 1;
	
	final int pageSize;
	final int pageShifts;
	final int chunkSize;
	final int directMemoryCacheAlignment;
	
	final int nSizes;
	final int nSubpages;
	final int nPSizes;
	final int lookupMaxSize;
	final int smallMaxSizeIdx;
	
	private final int[] pageIdx2sizeTab;
	private final int[] sizeIdx2sizeTab;
	
	private final int[] size2idxTab;
	
	SizeClasses(int pageSize, int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
		int group = log2(chunkSize) - LOG2_QUANTUM - LOG2_SIZE_CLASS_GROUP + 1;
		short[][] sizeClasses = new short[group << LOG2_SIZE_CLASS_GROUP][7];
		
		int normalMaxSize = -1;
		int nSizes = 0;
		@SuppressWarnings("unused")
		int size = 0;
		
		int log2Group = LOG2_QUANTUM;
		int log2Delta = LOG2_QUANTUM;
		int ndeltaLimit = 1 << LOG2_SIZE_CLASS_GROUP;
		
		for (int nDelta = 0; nDelta < ndeltaLimit; nDelta++, nSizes++) {
			short[] sizeClass = newSizeClass(nSizes, log2Group, log2Delta, nDelta, pageShifts);
			sizeClasses[nSizes] = sizeClass;
			size = normalMaxSize = sizeOf(sizeClass, directMemoryCacheAlignment);
		}
		
		assert chunkSize == normalMaxSize;
		
		int smallMaxSizeIdx = 0;
		int lookupMaxSize = 0;
		int nPSizes = 0;
		int nSubpages = 0;
		for (int idx = 0; idx < nSizes; idx++) {
			short[] sz = sizeClasses[idx];
			if (sz[PAGESIZE_IDX] == yes) {
				nPSizes++;
			}
			if (sz[SUBPAGE_IDX] == yes) {
				nSubpages++;
				smallMaxSizeIdx = idx;
			}
			if (sz[LOG2_DELTA_LOOKUP_IDX] != no) {
				lookupMaxSize = sizeOf(sz, directMemoryCacheAlignment);
			}
		}
		this.smallMaxSizeIdx = smallMaxSizeIdx;
		this.lookupMaxSize = lookupMaxSize;
		this.nPSizes = nPSizes;
		this.nSubpages = nSubpages;
		this.nSizes = nSizes;
		
		this.pageSize = pageSize;
		this.pageShifts = pageShifts;
		this.chunkSize = chunkSize;
		this.directMemoryCacheAlignment = directMemoryCacheAlignment;
		
		this.sizeIdx2sizeTab = newIdx2SizeTab(sizeClasses, nSizes, directMemoryCacheAlignment);
		this.pageIdx2sizeTab = newPageIdx2sizeTab(sizeClasses, nSizes, nPSizes, directMemoryCacheAlignment);
		this.size2idxTab = newSize2idxTab(lookupMaxSize, sizeClasses);
	}
	
	private static short[] newSizeClass(int index, int log2Group, int log2Delta, int nDelta, int pageShifts) {
		short isMultiPageSize;
		if (log2Delta >= pageShifts) {
			isMultiPageSize = yes;
		} else {
			int pageSize = 1 << pageShifts;
			int size = calculateSize(log2Group, nDelta, log2Delta);
			
			isMultiPageSize = size == size / pageSize * pageSize ? yes : no;
		}
		
		int log2Ndelta = nDelta == 0 ? 0 : log2(nDelta);
		
		byte remove = 1 << log2Ndelta < nDelta ? yes : no;
		int log2Size = log2Delta + log2Ndelta == log2Group ? log2Group + 1 : log2Group;
		if (log2Size == log2Group) {
			remove = yes;
		}
		
		short isSubpage = log2Size < pageShifts + LOG2_SIZE_CLASS_GROUP ? yes : no;
		
		int log2DeltaLookup = log2Size < LOG2_MAX_LOOKUP_SIZE ||
				log2Size == LOG2_MAX_LOOKUP_SIZE && remove == no 
				? log2Delta : no;
		return new short[] {
			(short) index, (short) log2Group, (short) log2Delta,
			(short) nDelta, isMultiPageSize, isSubpage, (short) log2DeltaLookup
		};
	}
	
	private static int[] newIdx2SizeTab(short[][] sizeClasses, int nSizes, int directMemoryCacheAlignment) {
		int[] sizeIdx2sizeTab = new int[nSizes];
		for (int i = 0; i < nSizes; i++) {
			short[] sizeClass = sizeClasses[i];
			sizeIdx2sizeTab[i] = sizeOf(sizeClass, directMemoryCacheAlignment);
		}
		return sizeIdx2sizeTab;
	}
	
	private static int calculateSize(int log2Group, int nDelta, int log2Delta) {
		return (1 << log2Group) + (nDelta << log2Delta);
	}
	
	private static int sizeOf(short[] sizeClass, int directMemoryCacheAlignment) {
		int log2Group = sizeClass[LOG2GROUP_IDX];
		int log2Delta = sizeClass[LOG2DELTA_IDX];
		int nDelta = sizeClass[NDELTA_IDX];
		
		int size = calculateSize(log2Group, nDelta, log2Delta);
		return alignSizeIfNeeded(size, directMemoryCacheAlignment);
	}
	
	private static int[] newPageIdx2sizeTab(short[][] sizeClasses, int nSizes, int nPSizes,
			int directMemoryCacheAlignment) {
		int[] pageIdx2sizeTab = new int[nPSizes];
		int pageIdx = 0;
		for (int i = 0; i < nSizes; i++) {
			short[] sizeClass = sizeClasses[i];
			if (sizeClass[PAGESIZE_IDX] == yes) {
				pageIdx2sizeTab[pageIdx++] = sizeOf(sizeClass, directMemoryCacheAlignment);
			}
		}
		return pageIdx2sizeTab;
	} 
	
	private static int[] newSize2idxTab(int lookupMaxSize, short[][] sizeClasses) {
		int[] size2idxTab = new int[lookupMaxSize >> LOG2_QUANTUM];
		int idx = 0;
		int size = 0;
		
		for (int i = 0; size <= lookupMaxSize; i++) {
			int log2Delta = sizeClasses[i][LOG2DELTA_IDX];
			int times = 1 << log2Delta - LOG2_QUANTUM;
			
			while (size <= lookupMaxSize && times-- > 0) {
				size2idxTab[idx++] = i;
				size = idx + 1 << LOG2_QUANTUM;
			}
		}
		return size2idxTab;
	}
	
	@Override
	public int sizeIdx2size(int sizeIdx) {
		return sizeIdx2sizeTab[sizeIdx];
	}

	@Override
	public int sizeIdx2sizeCompute(int sizeIdx) {
		int group = sizeIdx >> LOG2_SIZE_CLASS_GROUP;
		int mod = sizeIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;
		
		int groupSize = group == 0 ? 0 :
			1 << LOG2_QUANTUM + LOG2_SIZE_CLASS_GROUP - 1 << group;
		
		int shift = group == 0 ? 1 : group;
		int lgDelta = shift + LOG2_QUANTUM - 1;
		int modSize = mod + 1 << lgDelta;
		
		return groupSize + modSize;
	}

	@Override
	public long pageIdx2size(int pageIdx) {
		return pageIdx2sizeTab[pageIdx];
	}

	@Override
	public long pageIdx2sizeCompute(int pageIdx) {
		int group = pageIdx >> LOG2_SIZE_CLASS_GROUP;
		int mod = pageIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;
		
		long groupSize = group == 0 ? 0 :
			1L << pageShifts + LOG2_SIZE_CLASS_GROUP - 1 << group;
		
		int shift = group == 0 ? 1 : group;
		int log2Delta = shift + pageShifts - 1;
		int modSize = mod + 1 << log2Delta;
		
		return groupSize + modSize;
	}

	@Override
	public int size2SizeIdx(int size) {
		if (size == 0) {
			return 0;
		}
		if (size > chunkSize) {
			return nSizes;
		}
		size = alignSizeIfNeeded(size, directMemoryCacheAlignment);
		
		if (size <= lookupMaxSize) {
			return size2idxTab[size - 1 >> LOG2_QUANTUM];
		}
		
		int x = log2((size << 1) - 1);
		int shift = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1 
				? 0 : x - (LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM);
		
		int group = shift << LOG2_SIZE_CLASS_GROUP;
		int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
				? LOG2_QUANTUM : x - LOG2_SIZE_CLASS_GROUP - 1;
		int mod = size - 1 >> log2Delta & (1 << LOG2_SIZE_CLASS_GROUP) - 1;
		
		return group + mod;
	}

	@Override
	public int pages2pageIdx(int pages) {
		return pages2pageIdxCompute(pages, false);
	}

	@Override
	public int pages2pageIdxFloor(int pages) {
		return pages2pageIdxCompute(pages, true);
	}
	
	private int pages2pageIdxCompute(int pages, boolean floor) {
		int pageSize = pages << pageShifts;
		if (pageSize > chunkSize) {
			return nPSizes;
		}
		
		int x = log2((pageSize << 1) - 1);
		
		int shift = x < LOG2_SIZE_CLASS_GROUP + pageShifts 
				? 0 : x - (LOG2_SIZE_CLASS_GROUP + pageShifts);
		
		int group = shift << LOG2_SIZE_CLASS_GROUP;
		int log2Delta = x < LOG2_SIZE_CLASS_GROUP + pageShifts + 1 ?
				pageShifts : x - LOG2_SIZE_CLASS_GROUP - 1;
		
		int mod = pageSize - 1 >> log2Delta & (1 << LOG2_SIZE_CLASS_GROUP) - 1;
		
		int pageIdx = group + mod;
		
		if (floor && pageIdx2sizeTab[pageIdx] > pages << pageShifts) {
			pageIdx --;
		}
		return pageIdx;
	}
	
	private static int alignSizeIfNeeded(int size, int directMemoryCacheAlignment) {
		if (directMemoryCacheAlignment <= 0) {
			return size;
		}
		int delta = size & directMemoryCacheAlignment - 1;
		return delta == 0 ? size : size + directMemoryCacheAlignment - delta;
	}

	@Override
	public int normalizeSize(int size) {
		if (size == 0) {
			return sizeIdx2sizeTab[0];
		}
		size = alignSizeIfNeeded(size, directMemoryCacheAlignment);
		if (size <= lookupMaxSize) {
			int ret = sizeIdx2sizeTab[size2idxTab[size - 1 >> LOG2_QUANTUM]];
			assert ret == normalizeSizeCompute(size);
			return ret;
		}
		
		return normalizeSizeCompute(size);
	}
	
	private static int normalizeSizeCompute(int size) {
		int x = log2((size << 1) - 1);
		int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1 
				? LOG2_QUANTUM : x - LOG2_SIZE_CLASS_GROUP - 1;
		int delta = 1 <<  log2Delta;
		int delta_mask = delta - 1;
		return size + delta_mask & ~delta_mask;
	}
}
