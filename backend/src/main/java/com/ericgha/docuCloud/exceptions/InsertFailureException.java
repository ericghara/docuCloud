package com.ericgha.docuCloud.exceptions;

/**
 * Thrown when a required insert was not performed.
 */
public class InsertFailureException extends  RuntimeException {

    public InsertFailureException() {
        super();
    }

    public InsertFailureException(String message) {
        super( message );
    }

    public InsertFailureException(String message, Throwable cause) {
        super( message, cause );
    }

    public InsertFailureException(Throwable cause) {
        super( cause );
    }

    protected InsertFailureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
