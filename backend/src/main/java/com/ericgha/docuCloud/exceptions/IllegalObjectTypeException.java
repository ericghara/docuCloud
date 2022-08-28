package com.ericgha.docuCloud.exceptions;

/**
 * Thrown when expected a record of ObjectType x and got y.
 * Also thrown, in cases where ObjectType is null.
 */
public class IllegalObjectTypeException extends  RuntimeException {


    public IllegalObjectTypeException() {
        super();
    }

    public IllegalObjectTypeException(String message) {
        super( message );
    }

    public IllegalObjectTypeException(String message, Throwable cause) {
        super( message, cause );
    }

    public IllegalObjectTypeException(Throwable cause) {
        super( cause );
    }

    protected IllegalObjectTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
