package netty.channel;

public interface IoRegistration {
	<T> T attachment();
	
	long submit(IoOps ops);
	
	boolean isValid();
	
	boolean cancel();
}
