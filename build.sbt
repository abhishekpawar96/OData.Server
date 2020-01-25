scalaVersion := "2.12.3"

lazy val serviceCore = project.in(file("."))
	.aggregate(odataServiceJVM)
	.settings(exportJars := false,
		publishArtifact := false,
		publish := {},
		publishTo := None,
		publishLocal := {},
		test in assembly := {}
	)
	
lazy val odataServiceJVM = project.in(file("service"))
	.settings(
		name := "odata-service",
		libraryDependencies += "org.apache.olingo" % "odata-client-core" % "4.4.0",
		libraryDependencies += "org.apache.olingo" % "olingo-odata2-api" % "2.0.9",
		libraryDependencies += "org.apache.olingo" % "olingo-odata2-core" % "2.0.9",
		libraryDependencies += "org.scalatra" %% "scalatra-scalatest" % "2.6.1",
		libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",
		libraryDependencies += "org.apache.cxf" % "cxf-rt-frontend-jaxrs" % "3.1.14",
		libraryDependencies += "javax.xml.bind" % "jaxb-api" % "2.3.0",
		libraryDependencies += "javax.activation" % "javax.activation-api" % "1.2.0",
		test in assembly := {},
		assemblyMergeStrategy in assembly := {
			case PathList("mozilla","public-suffix-list.txt") => MergeStrategy.filterDistinctLines
			case PathList("META-INF","blueprint.handlers") => MergeStrategy.filterDistinctLines
			case PathList("org", "hamcrest", xs@_*) => MergeStrategy.filterDistinctLines
			case PathList("META-INF", "cxf", "bus-extensions.txt" ) => MergeStrategy.filterDistinctLines
			case x =>
				val oldStrategy = (assemblyMergeStrategy in assembly).value
				oldStrategy(x)
		},
		assemblyOutputPath in assembly := file("target/odata_service.jar")
	)

mainClass in assembly := Some("com.sap.marmolata.odata.localservice.v2.LocalServer")
