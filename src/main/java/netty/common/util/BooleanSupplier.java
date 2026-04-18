package netty.common.util;

public interface BooleanSupplier {
	
	boolean get() throws Exception;
	
	BooleanSupplier FALSE_SUPPLIER = new BooleanSupplier() {
		@Override
		public boolean get() {
			return false;
		}
	};
	
	BooleanSupplier TRUE_SUPPLIER = new BooleanSupplier() {
		@Override
		public boolean get() {
			return true;
		}
	};
	
}
