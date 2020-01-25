package odata.localservice.v2

import org.apache.olingo.odata2.api.edm.{EdmLiteralKind, EdmSimpleType}
import org.apache.olingo.odata2.api.ep.{EntityProvider, EntityProviderWriteProperties}
import org.apache.olingo.odata2.api.exception.{ODataException, ODataNotFoundException, ODataNotImplementedException}
import org.apache.olingo.odata2.api.processor.{ODataResponse, ODataSingleProcessor}
import org.apache.olingo.odata2.api.uri.info.{GetEntitySetCountUriInfo, GetEntitySetUriInfo, GetEntityUriInfo}
import org.apache.olingo.odata2.api.uri.{ExpandSelectTreeNode, KeyPredicate}

import scala.collection.JavaConverters._

private[v2] class OrderDetailsODataProcessor () extends ODataSingleProcessor {
  import OrderDetailsEdmProvider._

  val dataStore = new OrderDetailsDataStore
  import dataStore._

  @throws[ODataException]
  @throws[java.util.concurrent.ExecutionException]
  override def readEntitySet(uriInfo: GetEntitySetUriInfo, contentType: String): ODataResponse =
    uriInfo.getNavigationSegments.size match {
      case 0 =>
        val top = topValue(Left(uriInfo)).getOrElse(entitySize(Left(uriInfo)))
        val skip = skipValue(Left(uriInfo)).getOrElse(0)
        val entitySet = uriInfo.getStartEntitySet
        val selected = uriInfo.getSelect.asScala.map(_.getProperty.getName).asJava
        val sortedFilteredList = listSortedFiltered(Left(uriInfo), getEntitySet(entitySet.getName))
        val javaEntitySet = sortedFilteredList.slice(skip,top).map(_.asJava).asJava
        val entityProviderWriteProperties = EntityProviderWriteProperties
          .serviceRoot(getContext.getPathInfo.getServiceRoot)
          .expandSelectTree(ExpandSelectTreeNode.entitySet(entitySet)
            .selectedProperties(selected).build())
          .inlineCountType(uriInfo.getInlineCount)
          .inlineCount(sortedFilteredList.size)
          .build
        EntityProvider.writeFeed(contentType, entitySet, javaEntitySet, entityProviderWriteProperties)

      case 1 =>
        val entityProviderWriteProperties = EntityProviderWriteProperties
          .serviceRoot(getContext.getPathInfo.getServiceRoot).build
        uriInfo.getTargetEntitySet.getName match {
          case ENTITY_SET_NAME_CUSTOMERS => EntityProvider.writeFeed(contentType, uriInfo.getTargetEntitySet,
            getCustomersForProduct(getKeyValue(uriInfo.getKeyPredicates.get(0))).map(_.asJava).asJava,
            entityProviderWriteProperties)
          case ENTITY_SET_NAME_PRODUCTS => EntityProvider.writeFeed(contentType, uriInfo.getTargetEntitySet,
            getProductsForVendor(getKeyValue(uriInfo.getKeyPredicates.get(0))).map(_.asJava).asJava,
            entityProviderWriteProperties)
          case _=> throw new ODataNotFoundException(ODataNotFoundException.ENTITY)
        }
      case _=> throw new ODataNotImplementedException
    }

  @throws[ODataException]
  override def readEntity(uriInfo: GetEntityUriInfo, contentType: String): ODataResponse = uriInfo.getNavigationSegments.size match {
    case 0 => EntityProvider.writeEntry(contentType, uriInfo.getTargetEntitySet,
      getEntity(uriInfo.getStartEntitySet.getName,
        getKeyValue(uriInfo.getKeyPredicates.get(0))).asJava,
      EntityProviderWriteProperties.serviceRoot(getContext.getPathInfo.getServiceRoot).build)
    case 1 => uriInfo.getTargetEntitySet.getName match {
      case ENTITY_SET_NAME_PRODUCTS => EntityProvider.writeEntry(contentType,
        uriInfo.getTargetEntitySet, createProduct(getKeyValue(uriInfo.getKeyPredicates.get(0))).asJava,
        EntityProviderWriteProperties.serviceRoot(getContext.getPathInfo.getServiceRoot).build)
      case ENTITY_SET_NAME_PRODUCTS => EntityProvider.writeEntry(contentType,
        uriInfo.getTargetEntitySet, createCustomer(getKeyValue(uriInfo.getKeyPredicates.get(0))).asJava,
        EntityProviderWriteProperties.serviceRoot(getContext.getPathInfo.getServiceRoot).build)
      case _ => throw new ODataNotFoundException(ODataNotFoundException.ENTITY)
    }
    case _ => throw new ODataNotImplementedException
  }

  @throws[ODataException]
  private def getKeyValue(key: KeyPredicate) = {
    val property = key.getProperty
    val `type` = property.getType.asInstanceOf[EdmSimpleType]
    `type`.valueOfString(key.getLiteral, EdmLiteralKind.DEFAULT, property.getFacets, classOf[Integer])
  }

  @throws[ODataException]
  override def countEntitySet (uriInfo: GetEntitySetCountUriInfo, contentType: String): ODataResponse = {
    val entitySet = uriInfo.getStartEntitySet
    val top = topValue(Right(uriInfo)).getOrElse(entitySize(Right(uriInfo)))
    val skip = skipValue(Right(uriInfo)).getOrElse(0)

    ODataResponse.fromResponse(EntityProvider.writeText(s"${listSortedFiltered(Right(uriInfo),
      getEntitySet(entitySet.getName)).slice(skip, top).size}")).build
  }

  private def entitySize(uriInfoEither: Either[GetEntitySetUriInfo, GetEntitySetCountUriInfo ]): Int = uriInfoEither match {
    case Left(getUriInfo) =>
      val entitySet = getUriInfo.getStartEntitySet.getName
      if (Option(getUriInfo.getFilter).isDefined)
        listSortedFiltered(Left(getUriInfo), getEntitySet(entitySet)).size else getEntitySet(entitySet).size
    case Right(getUriInfo) =>
      val entitySet = getUriInfo.getStartEntitySet.getName
      if (Option(getUriInfo.getFilter).isDefined)
        listSortedFiltered(Right(getUriInfo), getEntitySet(entitySet)).size else getEntitySet(entitySet).size
  }

  private def skipValue (uriInfo: Either[GetEntitySetUriInfo, GetEntitySetCountUriInfo ]): Option[Int] = uriInfo match {
    case Left(getUriInfo)   => Option[Integer](getUriInfo.getSkip).map(_.toInt)
    case Right(getUriInfo)  => Option[Integer](getUriInfo.getSkip).map(_.toInt)
  }

  private def topValue (uriInfo: Either[GetEntitySetUriInfo, GetEntitySetCountUriInfo ]): Option[Int] = uriInfo match {
    case Left(getUriInfo)   => Option(getUriInfo.getTop).map(top => skipValue(uriInfo).getOrElse(0) + top)
    case Right(getUriInfo)  => Option(getUriInfo.getTop).map(top => skipValue(uriInfo).getOrElse(0) + top)
  }

  private def getEntitySet (entityName: String): List[Map[String, AnyRef]] = entityName match {
    case ENTITY_SET_NAME_CUSTOMERS => customers
    case ENTITY_SET_NAME_PRODUCTS => products
    case ENTITY_SET_NAME_VENDORS => vendors
    case ENTITY_SET_NAME_ORDERS => orders
    case ENTITY_SET_NAME_BILLS => bills
    case ENTITY_SET_NAME_PERSONS => persons
    case _ => throw new ODataNotFoundException(ODataNotFoundException.ENTITY)
  }

  private def getEntity (entityName: String, id: Integer): Map[String, AnyRef] = entityName match {
    case ENTITY_SET_NAME_CUSTOMERS => createCustomer(id)
    case ENTITY_SET_NAME_PRODUCTS => createProduct(id)
    case ENTITY_SET_NAME_VENDORS => createVendor(id)
    case ENTITY_SET_NAME_ORDERS => createOrder(id)
    case ENTITY_SET_NAME_BILLS => createBill(id)
    case ENTITY_SET_NAME_PERSONS => createPerson(id)
    case _ => throw new ODataNotFoundException(ODataNotFoundException.ENTITY)
  }
}