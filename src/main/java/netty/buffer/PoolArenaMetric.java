package netty.buffer;

import java.util.List;

public interface PoolArenaMetric extends SizeClassesMetric {
	
	int numThreadCaches();
	
	int numTinySubpages();
	
	int numSmallSubpages();
	
	int numChunkLists();
	
	List<PoolSubpageMetric> tinySubpages();
	
	List<PoolSubpageMetric> smallSubpages();
	
	List<PoolChunkListMetric> chunkLists();
	
	long numAllocations();
	
	long numTinyAllocations();
	
	long numSmallAllocations();
	
	long numNormalAllocations();
	
	long numHugeAllocations();
	
	default long numChunkAllocations() {
		return -1;
	}
	
	long numDeallocations();
	
	long numTinyDeallocations();
	
	long numSmallDeallocations();
	
	long numNormalDeallocations();
	
	long numHugeDeallocations();
	
	default long numChunkDeallocations() {
		return -1;
	}
	
	long numActiveAllocations();
	
	long numActiveTinyAllocations();
	
	long numActiveSmallAllocations();
	
	long numActiveNormalAllocations();
	
	long numActiveHugeAllocations();
	
	default long numActiveChunks() {
		return -1;
	}
	
	long numActiveBytes();
}
