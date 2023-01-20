package org.apache.pinot.broker.broker;

import org.apache.pinot.broker.api.ResultStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;


public class PinotFSResultStoreFactory extends ResultStoreFactory {

  private ResultStore _resultStore;

  public PinotFSResultStoreFactory() {
  }

  @Override
  public void init(PinotConfiguration configuration) {
    _resultStore = new PinotFSResultStore();
  }

  @Override
  public ResultStore create() {
    return _resultStore;
  }

  private static class PinotFSResultStore implements ResultStore {

    @Override
    public void initPagination(String pointer, RequestContext requestContext)
        throws Exception {

    }

    @Override
    public boolean uploadPaginationResult(String pointer, BrokerResponse brokerResponse) {
      return false;
    }

    @Override
    public BrokerResponse getPaginatedResult(String pointer, int offset, int fetch)
        throws Exception {
      return null;
    }

    @Override
    public boolean deletePaginationResult(String pointer) {
      return false;
    }
  }
}
