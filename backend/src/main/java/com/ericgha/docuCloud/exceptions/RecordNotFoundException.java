package com.ericgha.docuCloud.exceptions;

/**
 * Thrown when an operation could not be performed due to
 * a record not being found.
 */
public class RecordNotFoundException extends RuntimeException {

    public RecordNotFoundException() {
        super();
    }

    public RecordNotFoundException(String message) {
        super( message );
    }

    public RecordNotFoundException(String message, Throwable cause) {
        super( message, cause );
    }

    public RecordNotFoundException(Throwable cause) {
        super( cause );
    }

    protected RecordNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
