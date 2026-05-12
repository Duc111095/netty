package netty.channel;

public interface IoHandler {
	default void initialize() {
		
	}
	
	int run(IoHandlerContext context);
	
	default void prepareToDestroy() {
		
	}
	
	default void destroy() {
		
	}
	
	IoRegistration register(IoHandle handle) throws Exception;
	
	void wakeup();
	
	boolean isCompatible(Class<? extends IoHandle> handleType);
}
