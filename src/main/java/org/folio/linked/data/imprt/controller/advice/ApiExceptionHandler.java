package org.folio.linked.data.imprt.controller.advice;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.folio.spring.exception.NotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@Log4j2
@RestControllerAdvice
@RequiredArgsConstructor
public class ApiExceptionHandler {

  @ExceptionHandler({
    UnsupportedOperationException.class,
    MethodArgumentNotValidException.class,
    MethodArgumentTypeMismatchException.class,
    IllegalArgumentException.class,
    HttpMediaTypeNotSupportedException.class,
    MissingServletRequestParameterException.class,
    HttpMessageNotReadableException.class
  })
  public ResponseEntity<String> handleBadRequests(Exception e) {
    logException(e);
    return buildResponseEntity(e, BAD_REQUEST);
  }

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<String> handleNotFoundException(NotFoundException exception) {
    return buildResponseEntity(exception, NOT_FOUND);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleAllOtherExceptions(Exception exception) {
    logException(exception);
    return buildResponseEntity(exception, INTERNAL_SERVER_ERROR);
  }

  private ResponseEntity<String> buildResponseEntity(Exception e, HttpStatus status) {
    return ResponseEntity.status(status).body(String.valueOf(e));
  }

  private void logException(Exception exception) {
    log.log(Level.WARN, "Handling exception", exception);
  }
}
