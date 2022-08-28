package com.ericgha.docuCloud.exceptions;

public class DeleteFailureException extends RuntimeException {
    public DeleteFailureException() {
        super();
    }

    public DeleteFailureException(String message) {
        super( message );
    }

    public DeleteFailureException(String message, Throwable cause) {
        super( message, cause );
    }

    public DeleteFailureException(Throwable cause) {
        super( cause );
    }

    protected DeleteFailureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
