package org.apache.pinot.broker.api;

import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.trace.RequestContext;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ResultStore {

  void initPagination(String pointer, RequestContext requestContext) throws Exception;

  boolean uploadPaginationResult(String pointer, BrokerResponse brokerResponse);

  BrokerResponse getPaginatedResult(String pointer, int offset, int fetch) throws Exception;

  boolean deletePaginationResult(String pointer);
}
