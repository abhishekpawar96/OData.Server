package odata.localservice.v2

import java.util

import org.apache.olingo.odata2.api.edm.provider._
import org.apache.olingo.odata2.api.edm.{EdmMultiplicity, EdmSimpleTypeKind, FullQualifiedName}
import org.apache.olingo.odata2.api.exception.ODataException

private[v2] class OrderDetailsEdmProvider extends EdmProvider {
  import OrderDetailsEdmProvider._

  @throws[ODataException]
  override def getSchemas: util.List[Schema] = {
    val schemas = new util.ArrayList[Schema]
    val schema = new Schema

    schema.setNamespace(NAMESPACE)
    val entityTypes = new util.ArrayList[EntityType]
    entityTypes.add(getEntityType(ENTITY_TYPE_1_1))
    entityTypes.add(getEntityType(ENTITY_TYPE_1_2))
    entityTypes.add(getEntityType(ENTITY_TYPE_1_3))
    entityTypes.add(getEntityType(ENTITY_TYPE_1_4))
    entityTypes.add(getEntityType(ENTITY_TYPE_1_5))
    entityTypes.add(getEntityType(ENTITY_TYPE_1_6))
    schema.setEntityTypes(entityTypes)

    val associations = new util.ArrayList[Association]
    associations.add(getAssociation(ASSOCIATION_CUSTOMER_PRODUCT))
    schema.setAssociations(associations)

    val entityContainers = new util.ArrayList[EntityContainer]
    val entityContainer = new EntityContainer
    entityContainer.setName(ENTITY_CONTAINER).setDefaultEntityContainer(true)

    val entitySets = new util.ArrayList[EntitySet]
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_CUSTOMERS))
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_PRODUCTS))
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_VENDORS))
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_ORDERS))
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_BILLS))
    entitySets.add(getEntitySet(ENTITY_CONTAINER, ENTITY_SET_NAME_PERSONS))
    entityContainer.setEntitySets(entitySets)

    val associationSets = new util.ArrayList[AssociationSet]
    associationSets.add(getAssociationSet(ENTITY_CONTAINER, ASSOCIATION_CUSTOMER_PRODUCT, ENTITY_SET_NAME_CUSTOMERS, ROLE_1_1))
    entityContainer.setAssociationSets(associationSets)

    val functionImports = new util.ArrayList[FunctionImport]
    functionImports.add(getFunctionImport(ENTITY_CONTAINER, FUNCTION_IMPORT))
    entityContainer.setFunctionImports(functionImports)

    entityContainers.add(entityContainer)
    schema.setEntityContainers(entityContainers)
    schemas.add(schema)
    schemas
  }

  @throws[ODataException]
  override def getEntityType(edmFQName: FullQualifiedName): EntityType = {
    if (NAMESPACE == edmFQName.getNamespace)

      if (ENTITY_TYPE_1_1.getName == edmFQName.getName) {
        //Customer Properties

        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("Customer_Id").setType(EdmSimpleTypeKind.Int32)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Customer_Guid").setType(EdmSimpleTypeKind.Guid)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Customer_Name").setType(EdmSimpleTypeKind.String)
          .setFacets(new Facets().setNullable(false).setMaxLength(100)))
        properties.add(new SimpleProperty().setName("Customer_DOB").setType(EdmSimpleTypeKind.DateTime)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Customer_Points").setType(EdmSimpleTypeKind.Single))
        properties.add(new SimpleProperty().setName("Customer_Product_Id").setType(EdmSimpleTypeKind.Int32))

        //Navigation Properties
        val navigationProperties = new util.ArrayList[NavigationProperty]
        navigationProperties.add(new NavigationProperty().setName("Product")
          .setRelationship(ASSOCIATION_CUSTOMER_PRODUCT).setFromRole(ROLE_1_1).setToRole(ROLE_1_2))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("Customer_Id"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_1.getName)
          .setProperties(properties)
          .setKey(key)
          .setNavigationProperties(navigationProperties)
      }

      else if (ENTITY_TYPE_1_2.getName == edmFQName.getName) {

        //Product Properties
        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("Product_Id").setType(EdmSimpleTypeKind.Int16)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Product_Name").setType(EdmSimpleTypeKind.String)
          .setFacets(new Facets().setNullable(false).setMaxLength(100)))
        properties.add(new SimpleProperty().setName("Product_Price").setType(EdmSimpleTypeKind.Decimal))
        properties.add(new SimpleProperty().setName("Product_Is_Available").setType(EdmSimpleTypeKind.Boolean))
        properties.add(new SimpleProperty().setName("Product_Date").setType(EdmSimpleTypeKind.Time)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Product_Img").setType(EdmSimpleTypeKind.Binary))
        properties.add(new SimpleProperty().setName("Product_Updated").setType(EdmSimpleTypeKind.DateTimeOffset))
        properties.add(new SimpleProperty().setName("Product_Customer_Id").setType(EdmSimpleTypeKind.Int32))
        properties.add(new SimpleProperty().setName("Product_Vendor_Id").setType(EdmSimpleTypeKind.Int32))

        //Navigation Properties
        val navigationPropertiesCustomerProduct = new util.ArrayList[NavigationProperty]
        navigationPropertiesCustomerProduct.add(new NavigationProperty().setName("Customer")
          .setRelationship(ASSOCIATION_CUSTOMER_PRODUCT).setFromRole(ROLE_1_2).setToRole(ROLE_1_1))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("Product_Id"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_2.getName)
          .setProperties(properties)
          .setHasStream(true)
          .setKey(key)
          .setNavigationProperties(navigationPropertiesCustomerProduct)
      }

      else if (ENTITY_TYPE_1_3.getName == edmFQName.getName) {

        //Vendor Properties
        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("Vendor_Id").setType(EdmSimpleTypeKind.Int32)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Vendor_Name").setType(EdmSimpleTypeKind.String)
          .setFacets(new Facets().setNullable(false).setMaxLength(100)))
        properties.add(new SimpleProperty().setName("Vendor_Desc").setType(EdmSimpleTypeKind.Null))
        properties.add(new SimpleProperty().setName("Vendor_Rating").setType(EdmSimpleTypeKind.SByte))
        properties.add(new SimpleProperty().setName("Vendor_Contact").setType(EdmSimpleTypeKind.String))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("Vendor_Id"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_3.getName)
          .setProperties(properties)
          .setKey(key)
      }

      else if (ENTITY_TYPE_1_4.getName == edmFQName.getName) {

        //Order Properties
        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("Order_Id").setType(EdmSimpleTypeKind.Int64)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Order_Name").setType(EdmSimpleTypeKind.String)
          .setFacets(new Facets().setNullable(false).setMaxLength(100)))
        properties.add(new SimpleProperty().setName("Order_Price").setType(EdmSimpleTypeKind.Double)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Order_Quantity").setType(EdmSimpleTypeKind.Byte)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Order_Date").setType(EdmSimpleTypeKind.DateTime)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Order_Details").setType(EdmSimpleTypeKind.String)
          .setFacets(new Facets().setNullable(false)))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("Order_Id"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_4.getName).setProperties(properties).setKey(key)
      }

      else if (ENTITY_TYPE_1_5.getName == edmFQName.getName) {

        val entityAnnotation: AnnotationAttribute = new AnnotationAttribute

        entityAnnotation.setName("semantics")
        entityAnnotation.setText("aggregate")
        entityAnnotation.setPrefix("sap")
        entityAnnotation.setNamespace("http://www.sap.com/Protocols/SAPData")

        val entityAnnotationList = new util.ArrayList[AnnotationAttribute]
        entityAnnotationList.add(entityAnnotation)

        val dimensionAnnotation = new AnnotationAttribute
        dimensionAnnotation.setName("aggregation-role")
        dimensionAnnotation.setText("dimension")
        dimensionAnnotation.setPrefix("sap")
        dimensionAnnotation.setNamespace("http://www.sap.com/Protocols/SAPData")
        val aggregationDimensionAnnotation = new util.ArrayList[AnnotationAttribute]
        aggregationDimensionAnnotation.add(dimensionAnnotation)


        val measureAnnotation = new AnnotationAttribute
        measureAnnotation.setName("aggregation-role")
        measureAnnotation.setText("measure")
        measureAnnotation.setPrefix("sap")
        measureAnnotation.setNamespace("http://www.sap.com/Protocols/SAPData")
        val aggregationMeasureAnnotation = new util.ArrayList[AnnotationAttribute]
        aggregationMeasureAnnotation.add(measureAnnotation)

        //Bill Properties
        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("ID")
          .setType(EdmSimpleTypeKind.String).setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("Bill_Id")
          .setType(EdmSimpleTypeKind.String).setFacets(new Facets().setNullable(false)).setAnnotationAttributes(aggregationDimensionAnnotation))
        properties.add(new SimpleProperty().setName("Bill_Item")
          .setType(EdmSimpleTypeKind.String).setFacets(new Facets().setNullable(false)).setAnnotationAttributes(aggregationDimensionAnnotation))
        properties.add(new SimpleProperty().setName("Bill_Amount")
          .setType(EdmSimpleTypeKind.Double).setFacets(new Facets().setNullable(false)).setAnnotationAttributes(aggregationMeasureAnnotation))
        properties.add(new SimpleProperty().setName("NumberOfItems")
          .setType(EdmSimpleTypeKind.Double).setFacets(new Facets().setNullable(false)).setAnnotationAttributes(aggregationMeasureAnnotation))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("ID"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_5.getName).setProperties(properties).setKey(key).setAnnotationAttributes(entityAnnotationList)
      }

      else if (ENTITY_TYPE_1_6.getName == edmFQName.getName) {

        //Product Properties
        val properties = new util.ArrayList[Property]
        properties.add(new SimpleProperty().setName("ID").setType(EdmSimpleTypeKind.Int64)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("FirstName").setType(EdmSimpleTypeKind.String))
        properties.add(new SimpleProperty().setName("LastName").setType(EdmSimpleTypeKind.String))
        properties.add(new SimpleProperty().setName("PremiumCustomer").setType(EdmSimpleTypeKind.Boolean))
        properties.add(new SimpleProperty().setName("BirthDate").setType(EdmSimpleTypeKind.DateTime)
          .setFacets(new Facets().setNullable(false)))
        properties.add(new SimpleProperty().setName("NumberOfKids").setType(EdmSimpleTypeKind.Int32))
        properties.add(new SimpleProperty().setName("Height").setType(EdmSimpleTypeKind.Decimal))
        properties.add(new SimpleProperty().setName("CreditRating").setType(EdmSimpleTypeKind.Decimal))

        //Key
        val keyProperties = new util.ArrayList[PropertyRef]
        keyProperties.add(new PropertyRef().setName("ID"))
        val key = new Key().setKeys(keyProperties)

        return new EntityType().setName(ENTITY_TYPE_1_6.getName)
          .setProperties(properties)
          .setHasStream(true)
          .setKey(key)

      }

    null
  }

  @throws[ODataException]
  override def getEntitySet(entityContainer: String, name: String): EntitySet = {
    if (ENTITY_CONTAINER == entityContainer)
      if (ENTITY_SET_NAME_CUSTOMERS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_1)
      else if (ENTITY_SET_NAME_PRODUCTS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_2)
      else if (ENTITY_SET_NAME_VENDORS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_3)
      else if (ENTITY_SET_NAME_ORDERS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_4)
      else if (ENTITY_SET_NAME_BILLS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_5)
      else if (ENTITY_SET_NAME_PERSONS == name)
        return new EntitySet().setName(name).setEntityType(ENTITY_TYPE_1_6)

    null
  }

  @throws[ODataException]
  override def getEntityContainerInfo(name: String): EntityContainerInfo = {
    if (name == null || "ODataOrderDetailsContainer" == name)
      return new EntityContainerInfo().setName("ODataOrderDetailsContainer").setDefaultEntityContainer(true)
    null
  }

  @throws[ODataException]
  override def getFunctionImport(entityContainer: String, name: String): FunctionImport = {
    if (ENTITY_CONTAINER == entityContainer)
      if (FUNCTION_IMPORT == name)
        return new FunctionImport().setName(name)
          .setReturnType(new ReturnType().setTypeName(ENTITY_TYPE_1_1)
            .setMultiplicity(EdmMultiplicity.MANY)).setHttpMethod("GET")
    null
  }

  @throws[ODataException]
  override def getAssociation(edmFQName: FullQualifiedName): Association = {
    if (NAMESPACE == edmFQName.getNamespace)
      if (ASSOCIATION_CUSTOMER_PRODUCT.getName == edmFQName.getName)
        return new Association().setName(ASSOCIATION_CUSTOMER_PRODUCT.getName)
          .setEnd1(new AssociationEnd().setType(ENTITY_TYPE_1_1).setRole(ROLE_1_1).setMultiplicity(EdmMultiplicity.MANY))
          .setEnd2(new AssociationEnd().setType(ENTITY_TYPE_1_2).setRole(ROLE_1_2).setMultiplicity(EdmMultiplicity.ONE))
    null
  }

  @throws[ODataException]
  override def getAssociationSet(entityContainer: String,
                                 association: FullQualifiedName,
                                 sourceEntitySetName: String,
                                 sourceEntitySetRole: String): AssociationSet = {
    if (ENTITY_CONTAINER == entityContainer)
      if (ASSOCIATION_CUSTOMER_PRODUCT == association)
        return new AssociationSet().setName(ASSOCIATION_SET_1)
          .setAssociation(ASSOCIATION_CUSTOMER_PRODUCT)
          .setEnd1(new AssociationSetEnd().setRole(ROLE_1_2).setEntitySet(ENTITY_SET_NAME_PRODUCTS))
          .setEnd2(new AssociationSetEnd().setRole(ROLE_1_1).setEntitySet(ENTITY_SET_NAME_CUSTOMERS))
    null
  }
}

object OrderDetailsEdmProvider {

  val ENTITY_SET_NAME_CUSTOMERS     = "Customers"
  val ENTITY_SET_NAME_PRODUCTS      = "Products"
  val ENTITY_SET_NAME_VENDORS       = "Vendors"
  val ENTITY_SET_NAME_ORDERS        = "Orders"
  val ENTITY_SET_NAME_BILLS         = "Bills"
  val ENTITY_SET_NAME_PERSONS       = "Persons"

  val ENTITY_NAME_CUSTOMER    = "Customer"
  val ENTITY_NAME_PRODUCT     = "Product"
  val ENTITY_NAME_VENDOR      = "Vendor"
  val ENTITY_NAME_ORDER       = "Order"
  val ENTITY_NAME_BILL        = "Bill"
  val ENTITY_NAME_PERSON      = "Person"

  val NAMESPACE = "org.apache.olingo.odata2.ODataOrderDetails"

  val ENTITY_TYPE_1_1     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_CUSTOMER)
  val ENTITY_TYPE_1_2     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_PRODUCT)
  val ENTITY_TYPE_1_3     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_VENDOR)
  val ENTITY_TYPE_1_4     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_ORDER)
  val ENTITY_TYPE_1_5     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_BILL)
  val ENTITY_TYPE_1_6     = new FullQualifiedName(NAMESPACE, ENTITY_NAME_PERSON)

  val ASSOCIATION_CUSTOMER_PRODUCT  = new FullQualifiedName(NAMESPACE, "Customer_Product_Product_Customer")

  val ROLE_1_1 = "Customer_Product"
  val ROLE_1_2 = "Product_Customer"

  val ASSOCIATION_SET_1 = "Customers_Products"

  val ENTITY_CONTAINER = "ODataOrderDetailsContainer"
  val FUNCTION_IMPORT = "NumberOfCustomers"

}