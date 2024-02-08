package net.ucanaccess.exception;

public class FeatureNotSupportedRuntimeException extends UcanaccessRuntimeException {
    private static final long serialVersionUID = 1L;

    public FeatureNotSupportedRuntimeException() {
        super("Feature not supported");
    }

    public FeatureNotSupportedRuntimeException(String _feature) {
        super("Feature not supported: " + _feature);
    }
}
