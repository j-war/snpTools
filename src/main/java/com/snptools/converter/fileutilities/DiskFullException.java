package com.snptools.converter.fileutilities;

/**
 * Thrown when PrintWriter's checkError() returns true to
 * better understand its IOExceptions and expose potential
 * silent failures.
 */
public class DiskFullException extends Exception { 
    public DiskFullException(String errorMessage) {
        super(errorMessage);
    }
}
