package netty.common.util;

import netty.common.util.concurrent.Future;
import netty.common.util.concurrent.Promise;

public interface AsyncMapping<IN, OUT> {

	Future<OUT> map(IN input, Promise<OUT> promise);
}
