package netty.common.util;

import java.util.Set;

public interface Timer {
	
	Timeout newTimeout(TimerTask task, long delay, TimeUnit unit);
	
	Set<Timeout> stop();
}
