package netty.buffer;

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
	
	@Override
	public int sizeIdx2size(int sizeIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int sizeIdx2sizeCompute(int sizeIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int pageIdx2size(int pageIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int pageIdx2sizeComute(int pageIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int size2SizeIdx(int size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int pages2pageIdx(int pages) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int pages2pageIdxFloor(int pages) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int normalizeSize(int size) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
