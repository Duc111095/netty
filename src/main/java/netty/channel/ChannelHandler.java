package netty.channel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface ChannelHandler {
	void handlerAdded(ChannelHandlerContext ctx) throws Exception;
	
	void handlerRemoved(ChannelHandlerContext ctx) throws Exception;
	
	void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception;
	
	@Inherited
	@Documented
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	@interface Sharable {
		
	}
}
