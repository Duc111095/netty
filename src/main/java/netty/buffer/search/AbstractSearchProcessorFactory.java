package netty.buffer.search;

public abstract class AbstractSearchProcessorFactory implements SearchProcessorFactory {
	public static KmpSearchProcessorFactory newKmpSearchProcessorFactory(byte[] needle) {
		return new KmpSearchProcessorFactory(needle);
	}
	
	public static BitmapSearchProcessorFactory newBitmapSearchProcessorFactory(byte[] needle) {
		return new BitmapSearchProcessorFactory(needle);
	}
}
