package netty.channel;

public interface IoHandlerContext {
	boolean canBlock();
	
	long delayNanos(long currentTimeNanos);
	
	long deadlineNanos();
	
	default void reportActiveIoTime(long activeNanos) {
		
	}
	
	default boolean shouldReportActiveIoTime() {
		return false;
	}
}
