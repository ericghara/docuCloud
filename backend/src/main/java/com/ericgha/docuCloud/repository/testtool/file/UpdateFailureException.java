package com.ericgha.docuCloud.repository.testtool.file;

public class UpdateFailureException extends RuntimeException {

    public UpdateFailureException() {
        super();
    }

    public UpdateFailureException(String message) {
        super( message );
    }

    public UpdateFailureException(String message, Throwable cause) {
        super( message, cause );
    }

    public UpdateFailureException(Throwable cause) {
        super( cause );
    }

    protected UpdateFailureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
