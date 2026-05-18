package netty.channel;

public interface IoHandle extends AutoCloseable {
	
	void handle(IoRegistration registration, IoEvent ioEvent);
	
	default void registered() {
		
	}
	
	default void unregistered() {
		
	}
	
	void close() throws Exception;
}
