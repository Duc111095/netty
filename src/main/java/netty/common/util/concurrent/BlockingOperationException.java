package netty.common.util.concurrent;

public class BlockingOperationException extends IllegalStateException {

	private static final long serialVersionUID = 2815054078131020863L;
	public BlockingOperationException() {
		
	}
	
	public BlockingOperationException(String s) {
		super(s);
	}
	
	public BlockingOperationException(Throwable cause) {
		super(cause);
	}
	
	public BlockingOperationException(String message, Throwable cause) {
		super(message, cause);
	}
}
