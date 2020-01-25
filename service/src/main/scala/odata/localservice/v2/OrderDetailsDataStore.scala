package odata.localservice.v2

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, TimeZone, UUID}
import java.{lang, util}

import org.apache.olingo.odata2.api.edm.{EdmSimpleTypeKind, _}
import org.apache.olingo.odata2.api.exception.{ODataNotFoundException, ODataNotImplementedException}
import org.apache.olingo.odata2.api.uri.expression._
import org.apache.olingo.odata2.api.uri.info.{GetEntitySetCountUriInfo, GetEntitySetUriInfo}
import scala.collection.JavaConverters._

private[v2] class OrderDetailsDataStore {

  private def aggregateData(uriInfo: GetEntitySetUriInfo,
                            entitySet: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = {

    //check if column with aggregation-role = measure exists
    val isAgg = containsAggregation(uriInfo)

    if(isAgg) {
      // Extract $select fields
      val selectExpression = uriInfo.getSelect
      val ns = getNamespace(uriInfo).get
      val aggColumns = selectExpression.asScala.filter(x =>
        x.getProperty.getAnnotations.getAnnotationAttribute("aggregation-role", ns).getText.equals("measure")
      )

      val aggColumn = aggColumns.head.getProperty.getName
      // Create a tuple out of $select field names and types
      val selectPropertyTuple = selectExpression.asScala.map { selectedProperties =>
        (selectedProperties.getProperty.getType, selectedProperties.getProperty.getName)
      }

      // Identify if any numeric field
      val numericPropertyPresent = selectPropertyTuple.filter {
        property =>
          ((property._1 equals EdmSimpleTypeKind.Decimal.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Double.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Int16.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Int32.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Int64.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Byte.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.SByte.getEdmSimpleTypeInstance)
            || (property._1 equals EdmSimpleTypeKind.Single.getEdmSimpleTypeInstance))
      }

      val numericPresent = numericPropertyPresent.nonEmpty

      // Aggregate only if no primary key and a numeric field present
      if (numericPresent) {

        val aggColumnNames = aggColumns.map(_.getProperty.getName)
        val selectedAggColumns = numericPropertyPresent.map(_._2).filter(aggColumnNames.toSet)
        // Extract out all filed names
        val numericPropertyName = numericPropertyPresent.map(_._2).head

        // Tuple of full projection against $select projection
        (entitySet, entitySet.map(_.filterKeys(selectPropertyTuple.map(_._2).toList.contains(_)))) match {
          case (fullMap, subMap) =>

            // Project submap without numeric field to get duplicates
            val subMapWithoutNumeric = subMap.map(_.filterKeys(a => !(selectedAggColumns.contains(a))))

            // Create list of unique Map
            fullMap.groupBy { actualMap =>
              identity(subMapWithoutNumeric(fullMap.indexOf(actualMap)))
            }.map { case (index, listMap) =>

              // Merge duplicates if multiple entries
              if (listMap.length > 1) {

                val groupedVals = selectedAggColumns.map(col => {
                  val grouped = new lang.Double(
                    BigDecimal.valueOf {
                      listMap.map { entityMaps =>
                        s"${entityMaps(col)}".toDouble
                      }.foldLeft(new lang.Double(0.0))(_ + _)
                    }
                      .setScale(5).doubleValue()
                      .toString
                  )
                  (col, grouped)
                })

                // Aggregate the numeric field
                // Create new Map for aggregated value
                val singleMap = collection.mutable.Map() ++ listMap.head
                groupedVals.foreach(t => singleMap.update(t._1,t._2))

                // Mutable Map to Immutable Map
                singleMap.toMap
              }
              // Reuse existing if no duplicates
              else
                listMap.head
            }.toList
        }
      }
      // No Aggregation if no numeric field
      else
        entitySet
    }
    else entitySet
  }

  private def containsAggregation(uriInfo: GetEntitySetUriInfo): Boolean = {
    val selectedColumns = uriInfo.getSelect.asScala
    val nameSpace = getNamespace(uriInfo)
    nameSpace match{
      case Some(ns) => selectedColumns.exists(x => {
        x.getProperty.getAnnotations.getAnnotationAttribute("aggregation-role",ns).getText.equals("measure")
      })
      case _ => false
    }
  }

  private def getNamespace(uriInfo: GetEntitySetUriInfo): Option[String] = {
    val entitySet = uriInfo.getStartEntitySet
    val entityAnnotations = entitySet.getEntityType.getAnnotations.getAnnotationAttributes
    val semanticsAttribute =
      if(entityAnnotations == null) None else if(! entityAnnotations.isEmpty) {
        val temp = entityAnnotations.asScala.find(_.getName.equals("semantics"))
        if(temp.isDefined) Some(temp.get.getNamespace) else None}
      else None
    semanticsAttribute
  }

  def listSortedFiltered (uriInfo: Either[GetEntitySetUriInfo, GetEntitySetCountUriInfo ],
                          eSet: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = uriInfo match {
    case Left(getUriInfo)   =>
      val entitySet: List[Map[String, AnyRef]] = aggregateData(getUriInfo,eSet)
      val sortedEntitySet = if (Option(getUriInfo.getOrderBy).isDefined)
        evaluateMultipleOrderBy( entitySet, getUriInfo.getOrderBy.getOrders, 0) else entitySet

      if (Option(getUriInfo.getFilter).isDefined) sortedEntitySet.filter {
        evaluateFilter(_, getUriInfo.getFilter.getExpression).toBoolean} else sortedEntitySet
    case Right(getUriInfo)  => if (Option(getUriInfo.getFilter).isDefined) eSet.filter {
      evaluateFilter(_, getUriInfo.getFilter.getExpression).toBoolean} else eSet
  }

  def customers: List[Map[String, AnyRef]] = List (createCustomer(1), createCustomer(2), createCustomer(3),
    createCustomer(4), createCustomer(5), createCustomer(6))

  def products: List[Map[String, AnyRef]] = List (createProduct(1), createProduct(2), createProduct(3),
    createProduct(4), createProduct(5), createProduct(6), createProduct(7))

  def vendors: List[Map[String, AnyRef]] = List (createVendor(11111), createVendor(22222), createVendor(33333))

  def orders: List[Map[String, AnyRef]] = List (
    createOrder(100001), createOrder(100002), createOrder(100003), createOrder(100004), createOrder(100005),
    createOrder(100006), createOrder(100007), createOrder(100008), createOrder(100009), createOrder(200000))

  def bills: List[Map[String, AnyRef]] = List (createBill(1), createBill(2), createBill(3), createBill(4), createBill(5), createBill(6))

  def persons: List[Map[String, AnyRef]] = List (
    createPerson(1), createPerson(2), createPerson(3), createPerson(4), createPerson(5), createPerson(6),
    createPerson(7), createPerson(8), createPerson(9), createPerson(10), createPerson(11), createPerson(12),
    createPerson(13), createPerson(14), createPerson(15), createPerson(16), createPerson(17), createPerson(18),
    createPerson(19), createPerson(20), createPerson(21), createPerson(22), createPerson(23), createPerson(24),
    createPerson(25), createPerson(26), createPerson(27), createPerson(28), createPerson(29))

  def createCustomer (id: Int): Map[String, AnyRef] = {

    def customerMap (customerId: Int,
                     customerGuid: util.UUID ,
                     customerName: String,
                     customerDOB: Calendar,
                     customerCredit: lang.Double,
                     customerPId: Int) = Map(
      ("Customer_Id", customerId.asInstanceOf[java.lang.Integer]),
      ("Customer_Guid", customerGuid.asInstanceOf[util.UUID]),
      ("Customer_Name", customerName),
      ("Customer_DOB", customerDOB),
      ("Customer_Points", customerCredit.asInstanceOf[lang.Double]),
      ("Customer_Product_Id", customerPId.asInstanceOf[java.lang.Integer])
    )

    val updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    updated.set(Calendar.MILLISECOND,1)
    id match {
      case 1 => updated.set(2010, 0, 1, 1, 1, 1)
        customerMap(100, UUID.randomUUID(), "Customer Name String 1", updated, new lang.Double(50.0956132), 3)
      case 2 => updated.set(2011, 1, 2, 2, 2, 2)
        customerMap(2, UUID.randomUUID(), "Customer Name String 2", updated, null, 3)
      case 3 => updated.set(2012, 2, 3, 3, 3, 3)
        customerMap(3, UUID.randomUUID(), "Customer Name String 3", updated, new lang.Double( 19.0456747), 2)
      case 4 => updated.set(2013, 3, 4, 4, 4, 4)
        customerMap(4, UUID.randomUUID(), "Customer Name String 4", updated, new lang.Double( 45.1212337), 3)
      case 5 => updated.set(2014, 4, 5, 5, 5, 5)
        customerMap(5, UUID.randomUUID(), "Customer Name String 5", updated, null, 2)
      case 6 => updated.set(2015, 5, 6, 6, 6, 6)
        customerMap(6, UUID.randomUUID(),"Customer Name String 6", updated, null, 2)
      case _ => throw new IllegalArgumentException
    }
  }

  def createProduct (id: Int): Map[String, AnyRef] = {
    import java.math.BigDecimal

    def productMap (productId: Int,
                    productName: String,
                    productPrice: java.math.BigDecimal,
                    isAvailable: Boolean,
                    productDate: Calendar,
                    productImg: Array[Byte],
                    productUpdated: Calendar,
                    productCId: Int,
                    productVId: Int) = Map(
      ("Product_Id", productId.asInstanceOf[java.lang.Integer]),
      ("Product_Name", productName),
      ("Product_Price", productPrice.asInstanceOf[java.math.BigDecimal]),
      ("Product_Is_Available", isAvailable.asInstanceOf[java.lang.Boolean]),
      ("Product_Date", productDate),
      ("Product_Img", productImg),
      ("Product_Updated", productUpdated),
      ("Product_Customer_Id",productCId.asInstanceOf[java.lang.Integer]),
      ("Product_Vendor_Id",productVId.asInstanceOf[java.lang.Integer])
    )

    val updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    updated.set(Calendar.MILLISECOND,1)
    id match {
      case 1 => updated.set(2009, 8, 9, 9, 9, 9)
        productMap(1, "Product Name String 1", new BigDecimal(1989765.43), true, updated, "12ABFF".getBytes(), updated, 4, 33333)
      case 2 => updated.set(2008, 7, 8, 8, 8, 8)
        productMap(2, "Product Name String 2", new BigDecimal(8989765.99), true, updated, "12ABFF".getBytes(), updated, 2, 22222)
      case 3 => updated.set(2007, 6, 7, 7, 7, 7)
        productMap(3, "Product Name String 3", new BigDecimal(1976546.76), false, updated, "12ABFF".getBytes(), updated, 3, 22222)
      case 4 => updated.set(2006, 5, 6, 6, 6, 6)
        productMap(4, "Product Name String 4", new BigDecimal(5646508.23), false, updated, "12ABFF".getBytes(), updated, 4, 33333)
      case 5 => updated.set(2005, 4, 5, 5, 5, 5)
        productMap(5, "Product Name String 5", new BigDecimal(492765.78), true, updated, "12ABFF".getBytes(), updated, 4, 11111)
      case 6 => updated.set(2004, 3, 4, 4, 4, 4)
        productMap(6, "Product Name String 6", new BigDecimal(492765.78), true, updated, "12ABFF".getBytes(), updated, 5, 11111)
      case 7 => updated.set(2003, 2, 3, 3, 3, 3)
        productMap(7, "Product Name String 0", new BigDecimal(492765.78), true, updated, "12ABFF".getBytes(), updated, 6, 11111)
      case _ => throw new IllegalArgumentException
    }
  }

  def createVendor (id: Int): Map[String, AnyRef] = {
    def vendorMap (vendorId: Int,
                   vendorName: String,
                   vendorDesc: Null,
                   vendorRating: Byte,
                   vendorContact: String) = Map (
      ("Vendor_Id", vendorId.asInstanceOf[java.lang.Integer]),
      ("Vendor_Name", vendorName),
      ("Vendor_Desc", vendorDesc),
      ("Vendor_Rating", vendorRating.asInstanceOf[java.lang.Byte]),
      ("Vendor_Contact", vendorContact)
    )

    id match {
      case 11111 => vendorMap(11111, "Vendor Name String 1", null, -58, null)
      case 22222 => vendorMap(22222, "Vendor Name String 2", null, 96, "IND-KN")
      case 33333 => vendorMap(33333, "vendor Name String 3", null, 120, null)
      case _ => throw new IllegalArgumentException
    }
  }

  def createOrder (id: Int): Map[String, AnyRef] = {

    def orderMap (orderId: Int,
                  orderName: String,
                  orderPrice: java.lang.Double,
                  orderQuantity: Byte,
                  orderDate: Calendar,
                  orderDetails: String) = Map(
      ("Order_Id", orderId.asInstanceOf[java.lang.Integer]),
      ("Order_Name", orderName),
      ("Order_Price", orderPrice.asInstanceOf[java.lang.Double]),
      ("Order_Quantity", orderQuantity.asInstanceOf[java.lang.Byte]),
      ("Order_Date", orderDate),
      ("Order_Details", orderDetails)
    )

    val updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    updated.set(Calendar.MILLISECOND,1)

    id match {
      case 100001 => updated.set(2016, 0, 1, 1, 1, 1)
        orderMap(1001, "Order Name String 1", 10000000.10000000001, 10, updated, "Underscore _test String _ ")
      case 100002 => updated.set(2016, 1, 2, 2, 2, 2)
        orderMap(100002, "Order Name String 2", 20000000.20000000002, 20, updated, "String_ infix% operator")
      case 100003 => updated.set(2016, 2, 3, 3 ,3 ,3)
        orderMap(100003, "Order Name String 3", 30000000.30000000003, 30, updated, "_String infix % with space _")
      case 100004 => updated.set(2016, 3, 4, 4 ,4 ,4)
        orderMap(100004, "Order Name String 4", 40000000.40000000004, 40, updated, "%%String prefix oper")
      case 100005 => updated.set(2016, 4, 5, 5, 5, 5)
        orderMap(100005, "Order Name String 5", 50000000.50000000005, 50, updated, "String postfix oper%")
      case 100006 => updated.set(2016, 5, 6, 6, 6, 6)
        orderMap(100006, "Order Name String 6", 60000000.60000000006, 60, updated, "String_ post % oper %% ")
      case 100007 => updated.set(2016, 6, 7, 7, 7, 7)
        orderMap(100007, "Order Name String 7", 70000000.70000000007, 70, updated, "String infix _ and space")
      case 100008 => updated.set(2016, 7, 8, 8, 8, 8)
        orderMap(100008, "Order Name String 8", 80000000.80000000008, 80, updated, "_String prefix Oper")
      case 100009 => updated.set(2016, 8, 9, 9, 9, 9)
        orderMap(100009, "Order Name String 9", 90000000.90000000009, 90, updated, "String with post_")
      case 200000 => updated.set(2016, 9, 10, 10, 10, 10)
        orderMap(200000, "Order Name String 10", 100000000.1000000001, 100, updated, "String Normal Test")
      case _ =>throw new IllegalArgumentException
    }
  }

  def createBill (id: Int): Map[String, AnyRef] = {
    def billMap (billId: Int, billItem: String, billAmount: Double, numberOfItems: Double): Map[String , AnyRef] =
      Map (
        ("ID", (billId.toString  + "--" +billAmount.toString)),
        ("Bill_Id", billId.toString),
        ("Bill_Item", billItem.toString),
        ("Bill_Amount", billAmount.asInstanceOf[java.lang.Double]),
        ("NumberOfItems", numberOfItems.asInstanceOf[java.lang.Double])
      )
    id match {
      case 1 => billMap(1, "Item-1", 100.0, 2.0)
      case 2 => billMap(2, "Item-2", 200.0, 3.0)
      case 3 => billMap(3, "Item-3", 300.0, 4.0)
      case 4 => billMap(4, "Item-2", 400.0, 5.0)
      case 5 => billMap(5, "Item-4", 500.0, 6.0)
      case 6 => billMap(5, "Item-5", 5010.0, 7.0)
      case _ => throw new IllegalArgumentException
    }
  }

  def createPerson (id: Int): Map[String, AnyRef] = {

    def personMap (ID: Int,
                   FirstName: String ,
                   LastName: String,
                   PremiumCustomer: Boolean,
                   BirthDate: Calendar,
                   NumberOfKids: Int,
                   Height: lang.Double,
                   CreditRating: lang.Double): Map[String, AnyRef] = Map(
      ("ID", ID.asInstanceOf[java.lang.Integer]),
      ("FirstName", FirstName),
      ("LastName", LastName),
      ("PremiumCustomer", PremiumCustomer.asInstanceOf[java.lang.Boolean]),
      ("BirthDate", BirthDate),
      ("NumberOfKids", NumberOfKids.asInstanceOf[java.lang.Integer]),
      ("Height", Height.asInstanceOf[lang.Double]),
      ("CreditRating", CreditRating.asInstanceOf[lang.Double])
    )

    val updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    updated.set(Calendar.MILLISECOND,1)
    id match {
      case 1 => updated.set(2000, 0, 1, 1, 1, 1)
        personMap( 1," Keagan", "Christensen",  false, updated ,  0, new lang.Double( 1.53), new lang.Double( 36.71500))

      case 2 => updated.set(2001, 0, 1, 1, 1, 1)
        personMap( 2," Nathan", "Hernandez",  false, updated ,  1, new lang.Double( 1.55), new lang.Double( 98.63210))

      case 3 => updated.set(2002, 0, 1, 1, 1, 1)
        personMap( 3," Sariah", "Ballard",  false, updated ,  2, new lang.Double( 1.66), new lang.Double( 41.05130))

      case 4 => updated.set(2000, 5, 30, 1, 1, 1)
        personMap( 4," Dylan", "Branch",  false, updated  ,  3, new lang.Double( 1.75), new lang.Double( 81.60480))

      case 5 => updated.set(2001, 5, 30, 1, 1, 1)
        personMap( 5," Lincoln", "Keys",  false, updated,  3, new lang.Double( 1.76), new lang.Double( 31.68650))

      case 6 => updated.set(2002, 5, 30, 1, 1, 1)
        personMap( 6," Irene", "Flores",  false, updated,  2, new lang.Double( 1.88), new lang.Double( 14.76950))

      case 7 => updated.set(2000, 8, 15, 1, 1, 1)
        personMap( 7," Aubrie", "Moore",  false, updated,  0, new lang.Double( 1.68), new lang.Double( 57.12160))

      case 8 => updated.set(2001, 8, 15, 1, 1, 1)
        personMap( 8," Ernesto", "Williams",  false, updated,  1, new lang.Double( 1.58), new lang.Double( 31.88730))

      case 9 => updated.set(2002, 8, 15, 1, 1, 1)
        personMap( 9," Kaelyn", "Noble",  false, updated,  2, new lang.Double( 1.56), new lang.Double( 2.07736))

      case 10 => updated.set(2000, 0, 1, 1, 1, 1)
        personMap( 10," Devan", "Powell",  false, updated,  3, new lang.Double( 1.84), new lang.Double( 41.44130))

      case 11 => updated.set(2001, 0, 1, 1, 1, 1)
        personMap( 11," Jaylyn", "Rios",  false, updated,  3, new lang.Double( 1.57), new lang.Double( 42.37200))

      case 12 => updated.set(2002, 0, 1, 1, 1, 1)
        personMap( 12," Meadow", "Macias",  false, updated,  2, new lang.Double( 1.6), new lang.Double( 20.15290))

      case 13 => updated.set(2000, 5, 30, 1, 1, 1)
        personMap( 13," Raul", "James",  false, updated,  0, new lang.Double( 1.81), new lang.Double( 37.59120))

      case 14 => updated.set(2001, 5, 30, 1, 1, 1)
        personMap( 14," Hadley", "Mclaughlin",  false, updated,  1, new lang.Double( 1.76), new lang.Double( 67.84170))

      case 15 => updated.set(2002, 5, 30, 1, 1, 1)
        personMap( 15," Kaylie", "Casey",  false, updated,  2, new lang.Double( 1.74), new lang.Double( 28.08910))

      case 16 => updated.set(2000, 8, 15, 1, 1, 1)
        personMap( 16," Everett", "Chavez",  false, updated,  3, new lang.Double( 1.77), new lang.Double( 3.43588))

      case 17 => updated.set(2001, 8, 15, 1, 1, 1)
        personMap( 17," Javon", "Juarez",  true, updated,  3, new lang.Double( 1.72), new lang.Double( 70.50330))

      case 18 => updated.set(2002, 8, 15, 1, 1, 1)
        personMap( 18," Lucas", "Joseph",  true, updated,  2, new lang.Double( 1.6), new lang.Double( 45.38530))

      case 19 => updated.set(2000, 0, 1, 1, 1, 1)
        personMap( 19," Magdalena", "Roman",  true, updated,  0, new lang.Double( 1.67), new lang.Double( 26.62310))

      case 20 => updated.set(2001, 0, 1, 1, 1, 1)
        personMap( 20," Nathalie", "Gilmore",  true, updated,  1, new lang.Double( 1.53), new lang.Double( 78.27140))

      case 21 => updated.set(2002, 0, 1, 1, 1, 1)
        personMap( 21," Harper", "Moran",  true, updated,  2, new lang.Double( 1.8), new lang.Double( 39.62130))

      case 22 => updated.set(2000, 5, 30, 1, 1, 1)
        personMap( 22," Elisabeth", "Allison",  true, updated  ,  3, new lang.Double( 1.73), new lang.Double( 87.32150))

      case 23 => updated.set(2001, 5, 30, 1, 1, 1)
        personMap( 23," Seamus", "Preston",  true, updated,  3, new lang.Double( 1.71), new lang.Double( 97.63110))

      case 24 => updated.set(2002, 5, 30, 1, 1, 1)
        personMap( 24," Kobe", "Phillips",  true, updated,  2, new lang.Double( 1.55), new lang.Double( 65.39770))

      case 25 => updated.set(2000, 8, 15, 1, 1, 1)
        personMap( 25," Rylee", "Manning",  true, updated,  0, new lang.Double( 1.85), new lang.Double( 13.54700))

      case 26 => updated.set(2001, 8, 15, 1, 1, 1)
        personMap( 26," Julianna", "Bentley",  true, updated,  1, new lang.Double( 1.64), new lang.Double( 4.73784))

      case 27 => updated.set(2002, 8, 15, 1, 1, 1)
        personMap( 27," Jessica", "Eaton",  true, updated,  2, new lang.Double( 1.55), new lang.Double( 90.67190))

      case 28 => updated.set(2000, 0, 1, 1, 1, 1)
        personMap( 28," Malachi", "Decker",  true, updated,  3, new lang.Double( 1.76), new lang.Double( 65.48500))

      case 29 => updated.set(2001, 0, 1, 1, 1, 1)
        personMap( 29," Presley", "Frazier",  true, updated,  3, new lang.Double( 1.66), new lang.Double( 3.16087))

      case _ => throw new IllegalArgumentException
    }
  }

  def getCustomersForProduct(Product_Id: Int): List[Map[String, AnyRef]] = customers.filter(maps => maps("Customer_Product_Id") == Product_Id)

  def getProductForCustomer(Customer_Id: Int): Map[String, AnyRef] = products(s"""${createCustomer(Customer_Id)("Customer_Product_Id")}""".toInt)

  def getProductsForVendor(Vendor_Id: Int): List[Map[String, AnyRef]] = products.filter(maps => maps("Product_Vendor_Id") == Vendor_Id)

  def getVendorForProduct(Product_Id: Int) =    createVendor(s"""${createProduct(Product_Id)("Product_Vendor_Id")}""".toInt)

  private def evaluateMultipleOrderBy(entitySet: List[Map[String, AnyRef]],
                                      propertyList: util.List[OrderExpression],
                                      index: Int): List[Map[String, AnyRef]] = {
    val property = propertyList.get(index)
    val newProperty = property.getExpression.getUriLiteral
    val newOrder = property.getSortOrder.toString
    val newPropertyType = property.getExpression.getEdmType.toString
    val newEntitySet = sortMap(entitySet, newProperty, newOrder, newPropertyType)
    if (propertyList.size() == 1 | index == propertyList.size() - 1)
      newEntitySet
    else
      evaluateMultipleOrderBy(newEntitySet, propertyList, index + 1)
  }

  private def sortMap(entitySet: List[Map[String, AnyRef]],
                      property: String,
                      order: String,
                      propertyType: String): List[Map[String, AnyRef]] =
    entitySet.sortWith {
      (map1: Map[String, AnyRef], map2: Map[String, AnyRef]) => {
        val v1 = s"${map1(property)}"
        val v2 = s"${map2(property)}"

        val compareValue = (v1 , v2) match {
          case ("null", "null") => 0
          case ("null", _) => -1
          case (_, "null") => 1
          case (_, _) => propertyType match {
            case "Edm.String" | "Edm.Guid" => v1.compareTo(v2)
            case "Edm.Boolean" => v1.toBoolean.compareTo(v2.toBoolean)
            case "Edm.Double" | "Edm.SByte" | "Edm.Single" | "Edm.Byte" | "Edm.Decimal" =>
              new lang.Double(v1).compareTo(new lang.Double(v2))
            case "Edm.Int32" | "Edm.Int16" | "Edm.Int64" =>
              new lang.Long(v1).compareTo(new lang.Long(v2))
            case "Edm.DateTime" | "Edm.Time" | "Edm.DateTimeOffset" =>
              map1(property).asInstanceOf[Calendar].compareTo(map2(property).asInstanceOf[Calendar])
            case _ =>
              throw new ODataNotFoundException(ODataNotFoundException.ENTITY)
          }
        }

        if (order.compareToIgnoreCase("desc") == 0) compareValue > 0 else compareValue < 0
      }
    }

  private def evaluateFilter(entity: Map[String, AnyRef],
                             commonExpression: CommonExpression): String = {
    import lang.Double

    import org.apache.olingo.odata2.api.uri.expression.BinaryOperator._
    import org.apache.olingo.odata2.api.uri.expression.ExpressionKind._
    import org.apache.olingo.odata2.api.uri.expression.MethodOperator._
    import org.apache.olingo.odata2.api.uri.expression.UnaryOperator._

    def checkEdmType (operatorType: EdmType) =
      ((operatorType equals EdmSimpleTypeKind.String.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.DateTime.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.DateTimeOffset.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.Guid.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.Time.getEdmSimpleTypeInstance))

    def checkDateTimeEdmType (operatorType: EdmType) =
      ((operatorType equals EdmSimpleTypeKind.DateTime.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.DateTimeOffset.getEdmSimpleTypeInstance)
        || (operatorType equals EdmSimpleTypeKind.Time.getEdmSimpleTypeInstance))

    def checkNumericEdmType (binaryExpression: BinaryExpression) =
      ((binaryExpression.getEdmType ne EdmSimpleTypeKind.Decimal.getEdmSimpleTypeInstance)
        && (binaryExpression.getEdmType ne EdmSimpleTypeKind.Double.getEdmSimpleTypeInstance)
        && (binaryExpression.getEdmType ne EdmSimpleTypeKind.Single.getEdmSimpleTypeInstance))

    def dateComparision (left: String, right: String) = new Timestamp(
      new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(left).getTime).compareTo(new Timestamp(
      new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").parse(right).getTime))

    def createDateTime (value: String): Calendar = {
      val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
      val calendar = Calendar.getInstance()
      calendar.setTime(dateFormat.parse(value))
      calendar
    }

    def parseLong(value: String) = lang.Long.valueOf(value).longValue

    def parseDouble(value: String) = Double.valueOf(value).doubleValue

    def compareOperators(left: String, operatorType: EdmType, right: String) = if (checkEdmType(operatorType))
      parseDouble(s"${if (checkDateTimeEdmType(operatorType)) dateComparision(left, right) else left.compareTo(right)}") else parseDouble(left)

    commonExpression.getKind match {

      case UNARY => val unaryExpression = commonExpression.asInstanceOf[UnaryExpression]
        val operand         = evaluateFilter(entity, unaryExpression.getOperand)
        unaryExpression.getOperator match {
          case NOT      => lang.Boolean.toString(!lang.Boolean.parseBoolean(operand))
          case MINUS    => if (operand.startsWith("-")) operand.substring(1) else "-" + operand
          case _        => throw new ODataNotImplementedException
        }

      case BINARY => val binaryExpression  = commonExpression.asInstanceOf[BinaryExpression]
        val operatorType      = binaryExpression.getLeftOperand.getEdmType
        val left              = evaluateFilter(entity, binaryExpression.getLeftOperand)
        val right             = evaluateFilter(entity, binaryExpression.getRightOperand)

        binaryExpression.getOperator match {
          case ADD => Double.toString (if(checkNumericEdmType(binaryExpression) )
            (parseLong(left) + parseLong(right)).doubleValue() else parseDouble(left) + parseDouble(right))
          case SUB => Double.toString (if(checkNumericEdmType(binaryExpression) )
            (parseLong(left) - parseLong(right)).doubleValue() else parseDouble(left) - parseDouble(right))
          case MUL => Double.toString (if(checkNumericEdmType(binaryExpression) )
            (parseLong(left) * parseLong(right)).doubleValue() else parseDouble(left) * parseDouble(right))
          case DIV =>
            val number = Double.toString(parseDouble(left) / parseDouble(right))
            if (number.endsWith(".0")) number.replace(".0", "") else number
          case MODULO => Double.toString (if(checkNumericEdmType(binaryExpression) )
            (parseLong(left) % parseLong(right)).doubleValue() else parseDouble(left) % parseDouble(right))
          case AND =>
            lang.Boolean.toString(left == "true" && right == "true")
          case OR => lang.Boolean.toString(left == "true" || right == "true")
          case EQ =>  lang.Boolean.toString(if (checkDateTimeEdmType(operatorType))
            dateComparision(left, right) == 0 else left == "null" && right == "null" || left.equals(right))
          case NE => lang.Boolean.toString (if (checkDateTimeEdmType(operatorType))
            dateComparision(left, right) != 0 else (left != "null" || right != "null") && !(left == right))
          case LT =>
            lang.Boolean.toString (
              if(left == "null" || right == "null")
                false
              else {
                compareOperators(left, operatorType, right) < (
                  if (checkEdmType(operatorType))
                    0
                  else
                    parseDouble(right)
                  )
              }
            )
          case LE =>
            lang.Boolean.toString (
            if(left == "null" ^ right == "null")
              false
            else if(left == "null" && right == "null")
              true
            else {
              compareOperators(left, operatorType, right) <= (
                if (checkEdmType(operatorType))
                  0
                else
                  parseDouble(right)
                )
            }
          )
          case GT =>
            lang.Boolean.toString (
              if(left == "null" || right == "null")
                false
              else {
                compareOperators(left, operatorType, right) > (
                  if (checkEdmType(operatorType))
                    0
                  else
                    parseDouble(right)
                  )
              }
            )
          case GE =>
            lang.Boolean.toString (
              if(left == "null" ^ right == "null")
                false
              else if(left == "null" && right == "null")
                true
              else {
                compareOperators(left, operatorType, right) >= (
                  if (checkEdmType(operatorType))
                    0
                  else
                    parseDouble(right)
                  )
              }
            )
          case _ => throw new ODataNotImplementedException
        }

      case LITERAL =>
        val literal = commonExpression.asInstanceOf[LiteralExpression]
        val literalType = literal.getEdmType.asInstanceOf[EdmSimpleType]
        s"${
          literalType.valueToString(
            literalType.valueOfString(
              literal.getUriLiteral,
              EdmLiteralKind.URI,
              null,
              literalType.getDefaultType
            ),
            EdmLiteralKind.DEFAULT,
            null
          )
        }"

      case METHOD => val methodExpression  = commonExpression.asInstanceOf[MethodExpression]
        val first = evaluateFilter(entity, methodExpression.getParameters.get(0))
        val second = if (methodExpression.getParameterCount > 1) evaluateFilter(entity, methodExpression.getParameters.get(1)) else ""
        val third = if (methodExpression.getParameterCount > 2) evaluateFilter(entity, methodExpression.getParameters.get(2)) else ""

        methodExpression.getMethod match {
          case ENDSWITH   => lang.Boolean.toString(first.endsWith(second))
          case INDEXOF    => Integer.toString(first.indexOf(second))
          case STARTSWITH => lang.Boolean.toString(first.startsWith(second))
          case TOLOWER    => first.toLowerCase(Locale.ROOT)
          case TOUPPER    => first.toUpperCase(Locale.ROOT)
          case TRIM       => first.trim
          case SUBSTRING  =>
            val offset    = if (second.length == 0) 0 else Integer.parseInt(second)
            val length    = if (third.length == 0) 0 else Integer.parseInt(third)
            if (length == 0) first.substring(offset) else first.substring(offset, length)
          case SUBSTRINGOF=> lang.Boolean.toString(second.contains(first) || first.contains(second))
          case CONCAT     => first + second
          case LENGTH     => Integer.toString(first.length)
          case YEAR       => String.valueOf(createDateTime(first).get(Calendar.YEAR))
          case MONTH      => String.valueOf(createDateTime(first).get(Calendar.MONTH))
          case DAY        => String.valueOf(createDateTime(first).get(Calendar.DATE))
          case HOUR       => String.valueOf(createDateTime(first).get(Calendar.HOUR))
          case MINUTE     => String.valueOf(createDateTime(first).get(Calendar.MINUTE))
          case SECOND     => String.valueOf(createDateTime(first).get(Calendar.SECOND))
          case ROUND      => lang.Long.toString(Math.round(Double.valueOf(first)))
          case FLOOR      => lang.Long.toString(Math.round(Math.floor(Double.valueOf(first))))
          case CEILING    => lang.Long.toString(Math.round(Math.ceil(Double.valueOf(first))))
          case _          => throw new ODataNotImplementedException
        }

      case PROPERTY =>
        val property = commonExpression.asInstanceOf[PropertyExpression].getEdmProperty
        val propertyName      = property.getName
        if((property.getType.getName equals "DateTime")
          || (property.getType.getName equals "Time")
          || (property.getType.getName equals "DateTimeOffset")) {
          val calendarOriginal  = entity(propertyName).asInstanceOf[Calendar]
          val calendarTimeError     = calendarOriginal.getTime
          calendarTimeError.setHours( calendarOriginal.get(Calendar.HOUR))
          calendarTimeError.setMinutes( calendarOriginal.get(Calendar.MINUTE))
          calendarTimeError.setSeconds( calendarOriginal.get(Calendar.SECOND))
          calendarTimeError.toString
        } else {
          s"${entity(propertyName)}"
        }

      case _ => throw new ODataNotImplementedException
    }
  }

}