package netty.common.util;

public interface UncheckedBooleanSupplier extends BooleanSupplier {
	
	@Override
	boolean get();
	
	UncheckedBooleanSupplier FALSE_SUPPLIER = new UncheckedBooleanSupplier() {
		@Override
		public boolean get() {
			return false;
		}
	};
	
	UncheckedBooleanSupplier TRUE_SUPPLIER = new UncheckedBooleanSupplier() {
		@Override
		public boolean get() {
			return true;
		}
	};
}
