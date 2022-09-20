package com.ericgha.docuCloud.util;

import org.springframework.http.HttpStatus;

import java.util.Map;

final public class StatusCodeMapper {

    public static final HttpStatus DEFAULT = HttpStatus.INTERNAL_SERVER_ERROR;

    private StatusCodeMapper() throws IllegalAccessException {
        throw new IllegalAccessException( "Do not instantiate" );
    }

    /**
     * Maps a {@link Throwable} to an {@link HttpStatus}.  The Throwable {@code e} should exactly map to a {@link Class}
     * given in the {@code codeMap}. For example an {@code e} of {@link IllegalArgumentException} will not map to
     * {@link RuntimeException} although {@code IllegalArgumentException} is an instance of a {@code RuntimeException}.
     *
     * @param e           throwable to map to an {@link HttpStatus}
     * @param codeMap     map of Throwable {@link Class Class}es to {@link HttpStatus}
     * @param fallThrough catch all {@link HttpStatus}
     * @return {@link HttpStatus} the input throwable maps to
     */
    public static HttpStatus mapThrowable(Throwable e, Map<Class<? extends Throwable>, HttpStatus> codeMap, HttpStatus fallThrough) {
        return codeMap.getOrDefault( e.getClass(), fallThrough );
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0, HttpStatus fallThrough) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        return fallThrough;
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0,
                                          Class<? extends Throwable> throwable1, HttpStatus status1,
                                          HttpStatus fallThrough) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        if (throwable1.equals( e.getClass() )) {
            return status1;
        }
        return fallThrough;
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0,
                                          Class<? extends Throwable> throwable1, HttpStatus status1,
                                          Class<? extends Throwable> throwable2, HttpStatus status2,
                                          HttpStatus fallThrough) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        if (throwable1.equals( e.getClass() )) {
            return status1;
        }
        if (throwable2.equals( e.getClass() )) {
            return status2;
        }
        return fallThrough;
    }

    public static HttpStatus mapThrowable(Throwable e, Map<Class<? extends Throwable>, HttpStatus> codeMap) {
        return codeMap.getOrDefault( e.getClass(), DEFAULT );
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        return DEFAULT;
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0,
                                          Class<? extends Throwable> throwable1, HttpStatus status1) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        if (throwable1.equals( e.getClass() )) {
            return status1;
        }
        return DEFAULT;
    }

    public static HttpStatus mapThrowable(Throwable e, Class<? extends Throwable> throwable0, HttpStatus status0,
                                          Class<? extends Throwable> throwable1, HttpStatus status1,
                                          Class<? extends Throwable> throwable2, HttpStatus status2) {
        if (throwable0.equals( e.getClass() )) {
            return status0;
        }
        if (throwable1.equals( e.getClass() )) {
            return status1;
        }
        if (throwable2.equals( e.getClass() )) {
            return status2;
        }
        return DEFAULT;
    }
}
