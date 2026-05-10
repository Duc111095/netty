package netty.buffer;

import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Label("Buffer Reallocation")
@Name(ReallocateBufferEvent.NAME)
@Description("Triggered when a buffer is reallocated for resizing in an allocator. " +
        "Will be followed by an AllocateBufferEvent")
final class ReallocateBufferEvent extends AbstractBufferEvent {
    static final String NAME = "io.netty.ReallocateBuffer";
    private static final ReallocateBufferEvent INSTANCE = new ReallocateBufferEvent();
    
    public static boolean isEventEnabled() {
        return INSTANCE.isEnabled();
    }

    @DataAmount
    @Description("Targeted buffer capacity")
    public int newCapacity;
}