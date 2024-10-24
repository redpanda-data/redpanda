package io.vectorized.tx_verifier;

public class RetryableException extends Exception {
  public RetryableException(String msg) { super(msg); }
}