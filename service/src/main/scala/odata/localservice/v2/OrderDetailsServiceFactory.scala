package odata.localservice.v2

import org.apache.olingo.odata2.api.exception.ODataException
import org.apache.olingo.odata2.api.processor.ODataContext
import org.apache.olingo.odata2.api.{ODataService, ODataServiceFactory}

class OrderDetailsServiceFactory extends ODataServiceFactory {
  @throws[ODataException]
  override def createService(ctx: ODataContext): ODataService = {
    val edmProvider = new OrderDetailsEdmProvider
    val singleProcessor = new OrderDetailsODataProcessor
    createODataSingleProcessorService(edmProvider, singleProcessor)
  }
}
