package odata.localservice.v2

import org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet
import org.scalatra.test.EmbeddedJettyContainer

object LocalServer extends EmbeddedJettyContainer  {
  addServlet(new CXFNonSpringJaxrsServlet(), "/OrderDetailsODataSample.svc/*", "LocalODataServlet")
  val servlet  = servletContextHandler.getServletHandler.getServlet("LocalODataServlet")
  servlet.setInitParameter("javax.ws.rs.Application", "org.apache.olingo.odata2.core.rest.app.ODataApplication")
  servlet.setInitParameter("org.apache.olingo.odata2.service.factory", "odata.localservice.v2.OrderDetailsServiceFactory")
  servlet.setInitParameter("org.apache.olingo.odata2.service.factory.classloader", getClass.getClassLoader.toString)

  override def port: Int = 8181
  override def baseUrl: String = "localhost"

  def main(args: Array[String]): Unit = start()
}
