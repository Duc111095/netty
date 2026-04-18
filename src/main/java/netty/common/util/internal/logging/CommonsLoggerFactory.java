package netty.common.util.internal.logging;

import org.apache.commons.logging.LogFactory;

public class CommonsLoggerFactory extends InternalLoggerFactory{
	
	public static final InternalLoggerFactory INSTANCE = new CommonsLoggerFactory();

	public CommonsLoggerFactory() {}
	
	@Override
	public InternalLogger newInstance(String name) {
		return new CommonsLogger(LogFactory.getLog(name), name);
	}
}
