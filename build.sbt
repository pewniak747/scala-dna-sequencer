name := "sequencer-scala"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

fork in run := true

connectInput in run := true

javaOptions in run += "-Xss256m"
