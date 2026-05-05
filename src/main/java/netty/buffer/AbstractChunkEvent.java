package netty.buffer;

import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.MemoryAddress;

@SuppressWarnings("Since15")
abstract class AbstractChunkEvent extends AbstractAllocatorEvent {
	@DataAmount
	@Description("Size of chunk")
	public int capacity;
	@Description("Is this chunk referencing off-heap memory")
	public boolean direct;
	@Description("The memory address of the off-heap memory, if available")
	@MemoryAddress
	public long address;
	
	public void fill(ChunkInfo chunk, Class<? extends AbstractByteBufAllocator> allocatorType) {
		this.allocatorType = allocatorType;
		capacity = chunk.capacity();
		direct = chunk.isDirect();
		address = chunk.memoryAddress();
	}
}
