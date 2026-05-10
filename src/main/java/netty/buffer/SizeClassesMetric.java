package netty.buffer;

public interface SizeClassesMetric {
	
	int sizeIdx2size(int sizeIdx);
	
	int sizeIdx2sizeCompute(int sizeIdx);
	
	long pageIdx2size(int pageIdx);
	
	long pageIdx2sizeCompute(int pageIdx);
	
	int size2SizeIdx(int size);
	
	int pages2pageIdx(int pages);
	
	int pages2pageIdxFloor(int pages);
	
	int normalizeSize(int size);
}
