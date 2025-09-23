package org.folio.linked.data.imprt.controller.advice;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.folio.spring.exception.NotFoundException;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatusCode;

@UnitTest
class ApiExceptionHandlerTest {

  private final ApiExceptionHandler apiExceptionHandler = new ApiExceptionHandler();

  @Test
  void handleBadRequests_shouldReturnBadRequestResponse() {
    // given
    var e = new IllegalArgumentException("message");

    // when
    var result = apiExceptionHandler.handleBadRequests(e);

    // then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(400));
    assertThat(result.getBody()).isEqualTo("java.lang.IllegalArgumentException: message");
  }

  @Test
  void handleNotFoundException_shouldReturnBadRequestResponse() {
    // given
    var e = new NotFoundException("message");

    // when
    var result = apiExceptionHandler.handleNotFoundException(e);

    // then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(404));
    assertThat(result.getBody()).isEqualTo("org.folio.spring.exception.NotFoundException: message");
  }

  @Test
  void handleAllOtherExceptions_shouldReturnServerError() {
    // given
    var e = new NullPointerException("message");

    // when
    var result = apiExceptionHandler.handleAllOtherExceptions(e);

    // then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(500));
    assertThat(result.getBody()).isEqualTo("java.lang.NullPointerException: message");
  }
}
