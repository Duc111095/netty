package netty.buffer.search;

public interface MultiSearchProcessorFactory extends SearchProcessorFactory {
	
	@Override
	MultiSearchProcessor newSearchProcessor();
}
