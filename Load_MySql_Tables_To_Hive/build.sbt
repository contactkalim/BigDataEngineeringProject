name         := "LoadMySqlTablesToHive"
version      := "1.0"
organization := "ShadabKalim"
scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
resolvers += Resolver.mavenLocal
