package netty.buffer;

import jdk.jfr.Event;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;

@Enabled(false)
@Category("Netty")
@SuppressWarnings("Since15")
abstract class AbstractAllocatorEvent extends Event{
	@Label("Allocator Type")
	@Description("The type of allocator this event is for")
	public Class<? extends AbstractByteBufAllocator> allocatorType;
}
