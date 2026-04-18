package netty.common.util.internal.svm;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.RecomputeFieldValue;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(className = "io.netty.util.internal.RefCnt$UnsafeRefCnt")
final class RefCntSubstitution {
	
	private RefCntSubstitution() {}
	
	@Alias
	@RecomputeFieldValue(
			kind = RecomputeFieldValue.Kind.FieldOffset,
			declClassName = "io.netty.util.internal.RefCnt",
			name = "value"
	)
	public static long VALUE_OFFSET;
}
