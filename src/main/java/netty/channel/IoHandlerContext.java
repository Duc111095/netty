package netty.channel;

public interface IoHandlerContext {
	boolean canBlock();
	
	long delayNanos(long currentTimeNanos);
	
	long deadlineNanos();
	
	default void reportActiveToTime(long activeNanos) {
		
	}
	
	default boolean shouldReportActiveIoTime() {
		return false;
	}
}
