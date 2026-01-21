package org.folio.linked.data.imprt.batch.job.reader;

import jakarta.persistence.EntityManagerFactory;
import lombok.NonNull;
import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

public class DatabaseRdfLineItemReader extends AbstractItemStreamItemReader<RdfLineWithNumber> {

  private static final String SELECTION_QUERY =
    "SELECT r FROM RdfFileLine r WHERE r.jobExecutionId = :jobExecutionId ORDER BY lineNumber";
  private static final String QUERY_PARAM_JOB_EXECUTION_ID = "jobExecutionId";
  private final JpaCursorItemReader<RdfFileLine> delegate;

  public DatabaseRdfLineItemReader(Long jobExecutionId, EntityManagerFactory entityManagerFactory) {
    this.delegate = new JpaCursorItemReader<>();
    this.delegate.setEntityManagerFactory(entityManagerFactory);
    this.delegate.setQueryString(SELECTION_QUERY);
    this.delegate.setParameterValues(java.util.Map.of(QUERY_PARAM_JOB_EXECUTION_ID, jobExecutionId));
    this.delegate.setName("databaseRdfLineReader");
  }

  @Override
  public void open(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    delegate.open(executionContext);
  }

  @Override
  public RdfLineWithNumber read() throws Exception {
    var rdfLine = delegate.read();
    if (rdfLine == null) {
      return null;
    }
    return new RdfLineWithNumber(rdfLine.getLineNumber(), rdfLine.getContent());
  }

  @Override
  public void update(@NonNull ExecutionContext executionContext) throws ItemStreamException {
    delegate.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
    delegate.close();
  }
}
