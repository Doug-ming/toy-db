package com.jd.gaoming.storage.engine.exceptions;

public class DataAccessException extends RuntimeException{
    public DataAccessException() {

    }

    public DataAccessException(Throwable cause) {
        super(cause);
    }

    public DataAccessException(String message) {
        super(message);
    }

    public DataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
