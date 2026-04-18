package netty.common.util.internal.logging;

import org.apache.log4j.Logger;

public class Log4JLoggerFactory extends InternalLoggerFactory {
	public static final InternalLoggerFactory INSTANCE = new Log4JLoggerFactory();
	
	public Log4JLoggerFactory() {}
	
	@Override
	protected InternalLogger newInstance(String name) {
		return new Log4JLogger(Logger.getLogger(name));
	}

}
