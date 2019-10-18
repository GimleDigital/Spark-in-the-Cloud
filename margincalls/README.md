Instructions for running the Spark jobs:

1. You should previously have registered for a Databricks Community Edition
   account. You should also have installed SBT (Scala Build Tool) and Databricks
   CLI (Command Line Interface) in your PC.

2. Open a Windows Powershell, go to directory "./data" and run the following CLI
   commands:

   dbfs mkdirs dbfs:/margincalls/data
   dbfs cp -r . dbfs:/margincalls/data/

   Please bear in mind that the market data files are subject to copyright and
   should not be included in public repositories other than Gimlé Digital's.

3. Open a Windows Powershell, go to directory "./simplelog" and start SBT from
   there. Run the commands "compile" and "package" to create a jar file.

4. Open your web browser, enter your Databricks Community Edition account and
   create a cluster (when writing these instructions, version 6.0 with Scala
   2.11 and Spark 2.4.3 is preferred.

5. Enter the cluster and install the binary created in step 3, i.e.
   "./simplelog/target/scala-2.11/simplelog_2.11-0.1-SNAPSHOT.jar"

6. Browse to your Workspace and import the following files:

   ./DataCleaner.scala
   ./DealSampler.scala
   ./MarginCaller.scala

7. The imported Scala files will now be available as Notebooks. Attach these to
   the cluster and run them in alphabetical order.

8. Create a new directory and run CLI from there to retrieve the results:

   dbfs cp -r dbfs:/margincalls/data/margincalls.csv/ .
