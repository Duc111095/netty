package netty.buffer;

import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Buffer Allocation")
@Name(AllocateBufferEvent.NAME)
@Description("Triggered when a buffer is allocated (or reallocated) from an allocator")
final class AllocateBufferEvent extends AbstractBufferEvent {
	static final String NAME = "io.netty.AllocateBuffer";
	private static final AllocateBufferEvent INSTANCE = new AllocateBufferEvent();
	
	public static boolean isEventEnabled() {
		return INSTANCE.isEnabled();
	}
	
	@Description("Is this chunk pooled, or is it a one-off allocation for this buffer?")
	public boolean chunkPooled;
	@Description("Is this buffer's chunk part of a thread-local megazine or arena?")
	public boolean chunkThreadLocal;
}
