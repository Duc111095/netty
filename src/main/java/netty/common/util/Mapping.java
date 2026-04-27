package netty.common.util;

public interface Mapping<IN, OUT> {
	
	OUT map(IN input);
}
