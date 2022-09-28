# Databricks notebook source
# MAGIC %md
# MAGIC # [Introduction to Apache Spark](http://spark.apache.org/) 🎇🎇 ✨✨

# COMMAND ----------

# MAGIC %md
# MAGIC <center><img src="https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Apache_Spark_logo-f31fc9de-9456-4351-b459-2fe24f92628b.png"></center>
# MAGIC 
# MAGIC * Apache Spark in an open-source distributed cluster-computing system designed to be fast and general-purpose.
# MAGIC * Created in 2009 at Berkeley's AMPLab by Matei Zaharia, the Spark codebase was donated in 2013 to the Apache Software Foundation. It has since become one of its most active projects.
# MAGIC * Spark provides high-level APIs in `Scala`, `Java`, `Python` and `R` and an optimized execution engine. On top of this technology, sit higher-lever tools including `Spark SQL`, `MLlib`, `GraphX` and `Spark Streaming`.
# MAGIC 
# MAGIC ## What you will learn in this course 🧐🧐
# MAGIC This course will teach you some theory about the Spark framework, how it works and what advantages it has over other distributed computing frameworks. Here's the outline:
# MAGIC 
# MAGIC * Apache Spark
# MAGIC * Hadoop vs Spark
# MAGIC     * Faster through In-Memory computation
# MAGIC     * Simpler (high-level APIs) and execution engine optimisation
# MAGIC * Need for a DFS (Distributed File System)
# MAGIC * The Spark stack
# MAGIC     * Spark Core - the main functionnalities of the framework
# MAGIC     * Spark SQL - to handle structured data and run queries
# MAGIC     * GraphX - The visualisation tool of Spark
# MAGIC     * MLlib - The machine learning toolbox for Spark
# MAGIC     * Spark Streaming - An API to handle continuous inflow of data
# MAGIC 
# MAGIC * Spark's mechanics
# MAGIC     * DAG (Directed Acyclic Graphs) scheduling
# MAGIC     * Lazy execution
# MAGIC         * Transformations
# MAGIC         * Actions
# MAGIC     * Mixed language
# MAGIC * PySpark
# MAGIC 
# MAGIC ## Ressources 📚📚
# MAGIC * The [official spark documentation](http://spark.apache.org/docs/latest/)
# MAGIC * [cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html)
# MAGIC * Interesting notes on clusters: [https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-cluster.html](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-cluster.html)
# MAGIC * You can take a look at [Spark Basics : RDDs,Stages,Tasks and DAG](https://medium.com/@goyalsaurabh66/spark-basics-rdds-stages-tasks-and-dag-8da0f52f0454) but this covers concepts we haven't seen yet
# MAGIC * [Debugging PySpark](https://www.youtube.com/watch?v=McgG09XriEI)
# MAGIC - [What is Spark SQL](https://databricks.com/glossary/what-is-spark-sql)
# MAGIC - [Deep dive into Spark SQL's Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
# MAGIC - [SparkSqlAstBuilder](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-SparkSqlAstBuilder.html)
# MAGIC - [A Gentle Introduction to Stream Processing](https://medium.com/stream-processing/what-is-stream-processing-1eadfca11b97)
# MAGIC 
# MAGIC 
# MAGIC ## Hadoop vs Spark 🐘🆚✨
# MAGIC 
# MAGIC - **Faster through In-Memory computation** ⚡ Because memory time access is much faster than disk access, Spark's In-Memory computation makes it much faster than Hadoop which relies on disk
# MAGIC - **Simpler (high-level APIs) and execution engine optimisation 🧸 :** 
# MAGIC     * Spark's high-level APIs combined with lazy computation *means we don't have to optimize each query. Spark execution engine will take care of building an optimized physical execution plan.*
# MAGIC     * Also, code you write in "local" mode will work on a cluster "out-of-the-box" thanks to Spark's higher level API.
# MAGIC     * That doesn't mean it will be easy to write Spark code, but Spark makes it much easier to write optimized code that will run at big data scale.
# MAGIC 
# MAGIC ## The need for a distributed storage 🔀🔀
# MAGIC 
# MAGIC * If compute is distributed, all the machine needs to have access to the data, without a distributed storage that would be **very tedious**.
# MAGIC * Unlike Hadoop, Spark doesn't come with its own file system, but can interface with many existing ones, such as Hadoop Distributed File System (HDFS), Cassandra, Amazon S3 and many more...
# MAGIC * Spark can supports a pseudo-distributed local mode (for development or testing purposes), in this case, Spark is run on a single machine with one executor per CPU core and a distributed file storage is not required.
# MAGIC 
# MAGIC ## The Spark Stack ✨⚙️
# MAGIC 
# MAGIC One of Spark's promises is to deliver a unified analytics system. On top of its powerful distributed processing engine (Spark Core), sits a collection of higher-level libraries that all benefit from the improvements of the core library, which are low latency, and lazy execution.
# MAGIC 
# MAGIC *That's true in general, but can suffer from some caveats, in particular Spark Streaming's performances can't rival those of Storm and Flink which are other framework for running streaming jobs.*
# MAGIC 
# MAGIC <center><img src="https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/spark-stack-oreilly-674376df-ecdf-45f2-8ef7-539393568c0e.png"></center>
# MAGIC 
# MAGIC Source: Learning Spark (O'Reilly - Holden Karau, Andy Konwinski, Patrick Wendell & Matei Zaharia)
# MAGIC 
# MAGIC ### Spark Core 💖
# MAGIC 
# MAGIC Spark Core is the underlying general execution engine for the Spark platform that all other functionalities are built on top of.
# MAGIC 
# MAGIC It provides many core functionalities such as task dispatching and scheduling, memory management and basic I/O (input/output) functionalities, exposed through an application programming interface.
# MAGIC 
# MAGIC ### Spark SQL 🔢
# MAGIC 
# MAGIC Spark module for structured data processing.
# MAGIC 
# MAGIC Spark SQL provides a programming abstraction called DataFrame and can also act as a distributed SQL query engine. DataFrames are the other main data format in Spark. Spark DataFrames are column oriented, they have a data schema which describes the name and type of all the available columns. It allows for easier processing but adds contraints on the cleanliness and structure of the data.
# MAGIC 
# MAGIC Also they're called "DataFrames"
# MAGIC 
# MAGIC At the core of Spark SQL is the Catalyst optimizer, which leverages advanced programming language features (such as Scala’s pattern matching and quasi quotes) to build an extensible query optimizer.
# MAGIC 
# MAGIC <center><img src="https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Catalyst-Optimizer-diagram-152974c4-e1fc-4bb5-a788-c1ee71657ecd.png"></center>
# MAGIC 
# MAGIC 
# MAGIC Source: [https://databricks.com/glossary/catalyst-optimizer](https://databricks.com/glossary/catalyst-optimizer)
# MAGIC 
# MAGIC ### GraphX 📊
# MAGIC 
# MAGIC Spark module for Graph computations.
# MAGIC 
# MAGIC GraphX is a graph computation engine built on top of Spark that enables users to interactively build, transform and reason about graph structured data at scale. It comes with a library of common visualizations.
# MAGIC 
# MAGIC ### MLlib 🔮
# MAGIC 
# MAGIC Machine Learning library for Spark, inspired by Scikit-Learn (in particular, its pipelines system).
# MAGIC 
# MAGIC Historically a RDD-based API, it now comes with a DataFrame-based API that has become the primary API while the RDD-based API is now in [maintenance mode](https://spark.apache.org/docs/latest/ml-guide.html#announcement-dataframe-based-api-is-primary-api).
# MAGIC 
# MAGIC ### Spark streaming 🌊
# MAGIC 
# MAGIC Spark module for stream processing.
# MAGIC 
# MAGIC Streaming, also called Stream Processing is used to query continuous data stream and process this data within a small time period from the time of receiving the data. This is the opposite of batch processing, which occurs at a previously scheduled time independently from the data influx.
# MAGIC 
# MAGIC Spark Streaming uses Spark Core's fast scheduling capability to perform streaming analytics. It ingests data in mini-batches and performs RDD transformations on those mini-batches of data. This design enables the same set of application code written for batch analytics to be used in streaming analytics, this comes at the cost of having to wait for the full mini-batch to be processed while alternatives like Apache Storm and Apache Flink process data by event and provide better speed.
# MAGIC 
# MAGIC ## Spark mechanics & [cluster-overview](https://spark.apache.org/docs/latest/cluster-overview.html) ⚙️⚙️
# MAGIC 
# MAGIC > At a high level, every Spark application consists of a driver program that launches various parallel operations on a cluster. The driver program contains your application's main function and defines distributed datasets on the cluster, then applies operations on them.
# MAGIC - Learning Spark, page 14
# MAGIC 
# MAGIC <center><img src="https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/cluster-overview-273ddf73-9063-47bb-9060-e094443700eb.png" /></center>

# COMMAND ----------

# MAGIC %md
# MAGIC ## [DAG](https://medium.com/@goyalsaurabh66/spark-basics-rdds-stages-tasks-and-dag-8da0f52f0454) (Directed Acyclic Graph) Scheduling 📅

# COMMAND ----------

# MAGIC %md
# MAGIC * In order to distribute the execution among the worker nodes, Spark transforms the logical execution plan into a physical execution plan (how the computation will actually take place). While doing so, it implements an execution plan that will maximize performances, in particular avoiding moving data across the network, because as we've seen, network latency is the worse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lazy Execution 😴

# COMMAND ----------

# MAGIC %md
# MAGIC * A consequence of Spark's being so efficient when computing operations is lazy execution. This concept sets Spark (and therefore PySpark, the python API for using the Spark framework in python language) from classic python.
# MAGIC 
# MAGIC * meaning that an operation is not performed until an output is explicitly needed. For example, a join operation between two Spark dataframes will not immediately cause the join operation to be performed, which is how Pandas works. Instead, the join is performed once an output is added to the chain of operations to perform, such as displaying a sample of the resulting dataframe. One of the key differences between Pandas operations, where operations are eagerly performed and pulled into memory, is that PySpark operations are lazily performed and not pulled into memory until needed. One of the benefits of this approach is that the graph of operations to perform can be optimized before being sent to the cluster to execute.
# MAGIC 
# MAGIC * Python runs in what we call **eager execution**, meaning everytime you write some code and execute it, the operations your code is asking the computer to execute happen immediatly and the result is returned.
# MAGIC 
# MAGIC * In Spark things work a little differently, there are two types of operations: **transformations** and **actions**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations 🧙

# COMMAND ----------

# MAGIC %md
# MAGIC * **Transformations** are all operations that do not explicitely require the computer to return a result that should be stored, displayed or saved somewhere. These operations are only writen to the Graph waiting for an action to come up. For example, if you wish to calculate the frequency of all the the words in a set of text data, you may want to
# MAGIC 
# MAGIC 1. isolate each word,
# MAGIC 2. assign them a value of 1,
# MAGIC 3. group elements by key (meaning the word itself) so all occurences of the same words are grouped together,
# MAGIC 4. aggregate by summing the values associated with the words for each group.
# MAGIC 
# MAGIC * None of these operations require direct display or storage of a result, they just constitute in a roadmap plan that can be optimized whenever you request to see the result!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actions 🦸

# COMMAND ----------

# MAGIC %md
# MAGIC * **Actions** are operations that explicitely ask the computer to display or store the result of an operation. Taking our previous example, if we ask to see the complete list of words with their frequency, then all the previously mentionned transformation will actually execute one after the other. It can be very computing efficient because Spark knows all the operations that need to be done and can therefore plan accordingly, but additionnaly, if you're not looking to see the full result but just an extract to make sure the code runs correctly for example, then Spark will only work enough to give you want you want and stop (think of it as testing a piece of code on a sample instead of the full dataset for speed reasons).
# MAGIC 
# MAGIC * Lazy execution makes Spark very computing efficient, but it also makes it harder to debug when something goes wrong. Because only some errors can be detected when running transformations because Spark does not actually try to run the code. You can later be met with a runtime error when using an action later (when the code actually starts running), and if the result you get is not the one you expected, you'll need to go back and inspect every transformation to find out where something went wrong.
# MAGIC 
# MAGIC * Seems intimidating I know, but you can always set up actions like displaying the first few lines of data after each transformation in order to run sanity checks on what you are doing. It's a fair price to pay to be able to work with huge amounts of data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mixed language ☯️☯️

# COMMAND ----------

# MAGIC %md
# MAGIC * Apache Spark is written in `Scala`, making wide usage of the `Java Virtual Machine` and can be interfaced with: `Scala` (primary), `Java`, `Python` (`PySpark`) and `R`.
# MAGIC 
# MAGIC * Because Spark is written in `Scala`, `PySpark`, the `Python` interface tends to follow Scala's principle, whether for small details like naming convention (PySpark's API is frequently not consistent with Python's standard good practices, for example using pascalCase instead of snake_case) or global programming paradigm like functional programming.
# MAGIC 
# MAGIC * The functional paradigm is particularly adapted for distributed computing as it uses concept like immutability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark 🐍✨

# COMMAND ----------

# MAGIC %md
# MAGIC * PySpark is the Python API for Apache Spark. Powerful, but some caveats:
# MAGIC 
# MAGIC     - *Not as exhaustive as other's python libraries for data analysis and modeling (pandas, sklearn, etc..)*
# MAGIC     - *Will be slower than these on small data*
# MAGIC     - *Mixed language (harder to debug, common to find resources for Scala and not Python)*
# MAGIC 
# MAGIC * Debugging PySpark is hard:
# MAGIC 
# MAGIC     - *Debugging Distributed systems is hard*
# MAGIC     - *Debugging mixed languages is hard*
# MAGIC     - *Lazy evaluation can be difficult to debug*
# MAGIC 
# MAGIC > 💡 If you want an API closer to pandas while maintaining fast big data processing capabilities, take a look at [koalas](https://github.com/databricks/koalas) (still in beta) and [handyspark](https://towardsdatascience.com/handyspark-bringing-pandas-like-capabilities-to-spark-dataframes-5f1bcea9039e) (more robust).
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Install Spark (easy way : on Colab)

# COMMAND ----------

# !ls
# As you see, we don't have yet spark installed ! 
!apt-get update

# COMMAND ----------

# https://spark.apache.org/docs/latest/
# Spark runs on Java 8/11, Scala 2.12, Python 3.6+ and R 3.5+. Java 8 prior to version 8u92 support is deprecated as of Spark 3.0.0.!
# https://openjdk.java.net/
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
# Q : What's the difference : openjdk-normal Vs. openjdk-headless ? 
# R : openjdk-normal permet de créer des prog avec GUI. Ns n'avons pas besoin, ns allons lancer des calculs 
# https://stackoverflow.com/questions/24280872/difference-between-openjdk-6-jre-openjdk-6-jre-headless-openjdk-6-jre-lib

# COMMAND ----------

# Le dépôt des archives : 
# https://archive.apache.org/dist/spark/

!wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz

# COMMAND ----------

# On décompresse
!tar xf /content/spark-3.1.1-bin-hadoop2.7.tgz

# COMMAND ----------

!wget -q http://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
# On décompresse
!tar xf content/spark-2.3.1-bin-hadoop2.7.tgz

# COMMAND ----------

!pip install pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC * `findspark` pkg a pr obj : Find Spark Home, and initialize by adding `pyspark` to `sys.path`
# MAGIC     ```
# MAGIC     import findspark
# MAGIC     findspark.init()
# MAGIC     ```
# MAGIC     <https://stackoverflow.com/questions/36799643/pyspark-sparkcontext-name-error-sc-in-jupyter>

# COMMAND ----------

!pip install findspark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup environment

# COMMAND ----------

# We verify that spark is installed ! 
!ls

# COMMAND ----------

# We add env var
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop2.7" 

# COMMAND ----------

!pip install pyspark
# import findspark
# findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate() 
spark

# COMMAND ----------

from google.colab import drive
drive.mount('/content/drive')
from google.colab import files
files.upload()

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

os.listdir('/content/drive/MyDrive/data')

# COMMAND ----------

# MAGIC %md
# MAGIC # [Databricks Community](https://community.cloud.databricks.com/) 🧱🧱

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you will learn in this course 🧐🧐
# MAGIC * This course is a demo that will introduce to you one of the main data formats in spark which is spark RDDs (Resilient Distributed Datasets), we will walk you through how to use this low level data format using pyspark.
# MAGIC Here's the outline:
# MAGIC 
# MAGIC * Databricks
# MAGIC     * Login Page
# MAGIC     * Homepage
# MAGIC     * Workspace
# MAGIC     * Create Folder
# MAGIC     * Upload Notebook
# MAGIC     * Notebook View

# COMMAND ----------

# MAGIC %md
# MAGIC * Databricks is a cloud service provider which makes available clusters of machines with the spark framework already installed on them. Spark can be a real pain to set up, but gets amazing results once it's all up and running. We'll use databricks here so we can all work on a standardized environment!
# MAGIC 
# MAGIC * We'll use the community edition which is free, but limits the number and performance of the machines in our cluster. However this is not going to change a thing in terms of the code we'll right, whatever we'll learn here can scale up by connecting to a bigger cluster.
# MAGIC 
# MAGIC * Here's a walkthrough of what you should do once you are logged in ;)
# MAGIC 
# MAGIC ### Login Page 🔑
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_login.PNG)
# MAGIC 
# MAGIC ### Homepage 🏠
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_homepage.PNG)
# MAGIC 
# MAGIC ### Workspace 👷
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_workspace.PNG)
# MAGIC 
# MAGIC ### Create Folder 📁
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_create_folder.PNG)
# MAGIC 
# MAGIC ### Upload Notebook 📤
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_import_notebook.PNG)
# MAGIC 
# MAGIC ### Notebook View 📝
# MAGIC ![](https://full-stack-assets.s3.eu-west-3.amazonaws.com/images/Databricks/databricks_notebook_view.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC # Install Spark (en local)

# COMMAND ----------

import pandas as pd
# import findspark
# findspark.init(spark_home='/home/sayf/hadoop/spark')
# findspark.init(spark_home=r'C:/Users/bejao/AppData/Local/spark/spark-2.1.0-bin-hadoop2.7')

# COMMAND ----------

# MAGIC %md
# MAGIC # Chargement de Spark : [SparkSession](https://sparkbyexamples.com/pyspark/pyspark-what-is-sparksession/) & [SparkContext](https://sparkbyexamples.com/spark/how-to-create-a-sparksession-and-spark-context/)

# COMMAND ----------

from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# local[*] => On utilise les coeurs de la marchine
spark = SparkSession.builder.master("local[*]") \
                    .appName('spark') \
                    .getOrCreate()
# spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
sc = spark.sparkContext.setLogLevel('OFF')
# from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType, TimestampType, StructField, StructType
# import pyspark.sql.functions as F  

path = 'file:///mnt/c/Users/bejao/OneDrive/data/Crimes-2001_to_present.txt'
spark

# COMMAND ----------

SparkSession.getActiveSession()
# spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark - Dataframes 🗄️🗄️

# COMMAND ----------

# MAGIC %md
# MAGIC ## What will you learn in this course? 🧐🧐
# MAGIC 
# MAGIC * Running SQL queries against DataFrames
# MAGIC     * Select columns in Spark DataFrames
# MAGIC     * Actions
# MAGIC         * `.show()`
# MAGIC         * `.printSchema()`
# MAGIC         * `.take()`
# MAGIC         * `.collect()`
# MAGIC         * `.count()`
# MAGIC         * `.describe()`
# MAGIC         * `display()`
# MAGIC         * `.toPandas()`
# MAGIC         * `..write()`
# MAGIC     * Transformations
# MAGIC         * `.na`
# MAGIC         * `.fill()`
# MAGIC         * `.drop()`
# MAGIC         * `.isNull()`
# MAGIC         * `.replace()`
# MAGIC         * `.sql()`
# MAGIC         * `.select()`
# MAGIC         * `.alias(...)`
# MAGIC         * `.drop(...)`
# MAGIC         * `.limit()`
# MAGIC         * `.filter()`
# MAGIC         * `.selectExpr()`
# MAGIC         * `.dropDuplicates()`
# MAGIC         * `.distinct()`
# MAGIC         * `.orderBy()`
# MAGIC         * `.groupBy()`
# MAGIC         * `.withColumn()`
# MAGIC         * `.withColumnRenamed()`
# MAGIC         * Chaining everything together
# MAGIC         
# MAGIC * Some differences with pandas' DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC # Download and [preprocessing](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Viewing-Data) [Chicago's Reported Crime Data](https://data.cityofchicago.org/)

# COMMAND ----------

# Download the file
#!wget https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
# Rename the file 
#!mv rows.csv?accessType=DOWNLOAD reported-crimes.csv
#!ls
!pip install requests
!pip install json


# COMMAND ----------

import requests
import json
year=2001
data = {}
while year <= 2022:
    r = requests.get(f'https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=9223372036854775807&$where=year={year}').json()
    data[year]= spark.createDataFrame(r)
    print(year)
    year +=1

# COMMAND ----------

from functools import reduce 
from pyspark.sql import DataFrame
year = range(2001,2023)
dfs = []
for y in year:
    dfs.append(data.get(y))
dfs
df_concat = reduce(DataFrame.union, dfs)
display(df_concat)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_concat.count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_list =[]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

print(data)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Read a huge csv : [doc offic](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture avec Python : 

# COMMAND ----------

import pandas as pd
pd.read_csv(path)
# Le lecture est très coûteuse en temps et en mémoire. Il faut passer par un outil dédié à la Big Data => Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture avec Spark

# COMMAND ----------

# MAGIC %md
# MAGIC [read.csv](https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv), [read.json](https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json), [read.parquet](https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.parquet), option [InferSchema](https://www.learntospark.com/2020/10/spark-optimization-technique-inferschema.html)

# COMMAND ----------

# Lecture rapide (sans spécifier les options nécessaires)
df = spark.createDataFrame(data)

# Lecture avec l'option 'header=True' (pour rajouter les noms de colonnes à votre df)
# df = spark.read.csv(path, header=True)
# df.rdd.getNumPartitions()

# Lecture avec l'option 'inferSchema' (plus coûteuse : 30s). Elle permet de transformer les colonnes en types plus précis : int  / boolean / string / double...
# bien sûr spark trouve les types uniquement si le fichier d'origine permet de les trouver de manière simple

# df = spark.read.csv(path, header=True, inferSchema=True)

#df = spark.read.format("csv").option("header","true")\
#                                   .option("delimiter", ",")\
#                                   .load(path)


# df #  Affichage intelligent, uniquement les cols et leur type, contexte Big Data (pas le df en entier )
# df.show(5)
# df.show(1, vertical=True)
# https://sqlrelease.com/show-full-column-content-in-spark
# df.show(1, vertical=True, truncate=False)

display(df.count())
#df.dtypes
# type(df.dtypes)
#pd.DataFrame(df.dtypes, columns=['col', 'type']).value_counts('type')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df1 = df.drop(":@computed_region_43wa_7qmu",":@computed_region_awaf_s7ux", ":@computed_region_6mkv_f3dw", ':@computed_region_vrxf_vc4k', ':@computed_region_rpca_8um6',':@computed_region_d3ds_rm58',
 ':@computed_region_d9mm_jgwp', ':@computed_region_bdys_3d7i')
display(df1)
df1.columns


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # [Affichage comme dans un notebook natif](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html#Viewing-Data)

# COMMAND ----------

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df

# COMMAND ----------

# MAGIC %md
# MAGIC # Q : Déterminer le bon [schéma](https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/)

# COMMAND ----------

# MAGIC %md
# MAGIC * Laisser Spark déterminer (pour nous) les bons types des cols est très coûteux parcq le df d'origine contient + de 5M rows => `spark.read.csv(path, header=True, InferSchema=True)` 
# MAGIC * Ns allons le faire manuellement (suivant notre intuition)

# COMMAND ----------

# Par défaut, Spark considère toutes les cols en String
# df.dtypes
# pd.DataFrame(df.dtypes, columns=['col', 'type']).T
pd.DataFrame(df.dtypes, columns=['col', 'type']).value_counts('type')
# df.printSchema()


# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType, TimestampType, StructField, StructType 

# COMMAND ----------

# cols = df.columns
cols  = ['ID', 'Case Number', 'Date', 'Block', 'IUCR', 'Primary Type', 'Description', 'Location Description', 'Arrest', 'Domestic', 'Beat', 'District', 'Ward', 'Community Area', 'FBI Code', 'X Coordinate', 'Y Coordinate', 'Year', 'Updated On', 'Latitude', 'Longitude', 'Location', 'Historical Wards 2003-2015', 'Zip Codes', 'Community Areas', 'Census Tracts', 'Wards', 'Boundaries - ZIP Codes', 'Police Districts', 'Police Beats']

types = [IntegerType(), StringType(), StringType(), StringType(), StringType(), StringType(), StringType(), StringType(), BooleanType(), BooleanType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), StringType(), IntegerType(), IntegerType(), IntegerType(), StringType(), DoubleType(), DoubleType(), StringType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType()]

# les cols `Dates` sont conservés en StringType et seront convertit en TimestampType parce qu'elle nécessitent un traitement spécifique.

# On crée une liste de tuples avec `zip`
cols_types = list(zip(cols, types))

# On ajoute `True` dans les tuples à l'aide de comprehension list
schema = StructType([ StructField(i[0], i[1], True) for i in cols_types])
schema

# COMMAND ----------

cols_types

# COMMAND ----------

path = 'file:///mnt/c/Users/bejao/OneDrive/data/Crimes-2001_to_present.txt'
df = spark.read.csv(path, header=True, schema=schema)
df
# pd.DataFrame(df.dtypes, columns=['col', 'type']).value_counts('type')

# COMMAND ----------

StructType([StructField(ID,StringType,true),
    StructField(Case Number,StringType,true),StructField(Date,TimestampType,true),StructField(Block,StringType,true),StructField(IUCR,StringType,true),StructField(Primary Type,StringType,true),StructField(Description,StringType,true),StructField(Location Description,StringType,true),StructField(Arrest,StringType,true),StructField(Domestic,BooleanType,true),StructField(Beat,StringType,true),StructField(District,StringType,true),StructField(Ward,StringType,true),StructField(Community Area,StringType,true),StructField(FBI Code,StringType,true),StructField(X Coordinate,StringType,true),StructField(Y Coordinate,StringType,true),StructField(Year,IntegerType,true),StructField(Updated On,StringType,true),StructField(Latitude,DoubleType,true),StructField(Longitude,DoubleType,true),StructField(Location,StringType,true),StructField(Historical Wards 2003-2015,StringType,true),StructField(Zip Codes,StringType,true),StructField(Community Areas,StringType,true),StructField(Census Tracts,StringType,true),StructField(Wards,StringType,true),StructField(Boundaries - ZIP Codes,StringType,true),StructField(Police Districts,StringType,true),StructField(Police Beats,StringType,true))])


# COMMAND ----------

# MAGIC %md
# MAGIC # head, tail, take, limit, collect, show ? 

# COMMAND ----------

# limit() retourne un df-spark que ns pouvons afficher avec show, que je peux convertir en df-pandas => affichage plus agréable 
# https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-to-pandas/
# df.limit(5).toPandas()

# pd.DataFrame(df.head(5), columns=df.columns)
# df.head(5) #  contrairement à show(), head() affiche une liste de Row
# df.take(5) #  idem
# type(df.head(5)) #  liste de row (à la spark)
# type(df.head(5)[0])

# df.limit(5).collect() #  collect après limit retourne une liste de Row
df.collect() # ne pas utiliser parcq retourner tt le dataset (si vs avez du vrai Big Data)

# COMMAND ----------

# df.tail(5) 
import pandas as pd
pd.DataFrame(df.tail(5), columns=df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # shape

# COMMAND ----------

# df.columns
# type(df.columns) #  liste
# len(df.columns)
df.count()   # nb de ligne : 6899950

# COMMAND ----------

# MAGIC %md
# MAGIC # [cache or persist](https://stackoverflow.com/questions/26870537/what-is-the-difference-between-cache-and-persist?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa) ? 

# COMMAND ----------

# MAGIC %md
# MAGIC * Comme ns allons réaliser de nombreuses opé sur le `df`, il vaut mieux le mettre en `cache` ce qui permettra de gagner du temps de calcul. Le `df` sera stocké en mémoire vive
# MAGIC * The difference between `cache` and `persist` is purely syntactic. `cache` is a synonym of `persist` or `persist(MEMORY_ONLY)`. But Persist() We can save the intermediate results in 5 storage levels : `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`

# COMMAND ----------

df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # [describe](https://databricks.com/fr/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html) & [summary](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.summary.html#pyspark.sql.DataFrame.summary) 

# COMMAND ----------

# MAGIC %md
# MAGIC * L'opé suivante va mettre approx 3 min et réaliser 14 `tasks` !
# MAGIC * Que signifie `stage` dans Spark ?  : [sackoverflow](https://stackoverflow.com/questions/32994980/what-does-stage-mean-in-the-spark-logs)
# MAGIC * Que signifie tasks  : [mallikarjuna](https://mallikarjuna_g.gitbooks.io/spark/content/spark-taskscheduler-tasks.html), [stackoverflow](https://stackoverflow.com/questions/25276409/what-is-a-task-in-spark-how-does-the-spark-worker-execute-the-jar-file)

# COMMAND ----------

df_desc = df.describe()
df_desc
# df_desc.show()
# df_desc = df.describe().toPandas()
# df_desc.set_index('summary').T 


# COMMAND ----------

# MAGIC %md
# MAGIC * L'opé suivante va mettre approx 7 min et réaliser 14 `tasks` !

# COMMAND ----------

df.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC # Missing values : [sparkByExamples](https://sparkbyexamples.com/pyspark/pyspark-find-count-of-null-none-nan-values/), [stackoverflow](https://stackoverflow.com/questions/44413132/count-the-number-of-missing-values-in-a-dataframe-spark)

# COMMAND ----------

# df.summary('count').toPandas().T

# [{c:df.filter(df[c].isNull()).count()} for c in df.columns]

# from pyspark.sql.functions import isnull, when, count, col
# df_na = df.select([count(when(isnull(c))).alias(c) for c in df.columns]).toPandas()
# df_na.T.sort_values(by = 0, ascending=False)/df.count() * 100

# COMMAND ----------

# MAGIC %md
# MAGIC # Unique values : [SparkByExample](https://sparkbyexamples.com/pyspark/pyspark-distinct-to-drop-duplicates/), [stackoverflow](https://stackoverflow.com/questions/39383557/show-distinct-column-values-in-pyspark-dataframe)

# COMMAND ----------

df_unique = df.select('*').distinct().show(10, truncate=False)


# [{c:df.select(c).distinct().count()} for c in df.columns]

# COMMAND ----------

# MAGIC %md
# MAGIC # [value_counts](https://napsterinblue.github.io/notes/spark/sparksql/value_counts/)   [stackoverflow](https://stackoverflow.com/questions/51063624/whats-the-equivalent-of-pandas-value-counts-in-pyspark)

# COMMAND ----------

# import pyspark.sql.functions as F     

# df_value_counts = df.select('Primary Type').groupBy('Primary Type').count().orderBy('count', ascending=False)# .show(truncate = False)
# toto = df_value_counts.agg({'count':'sum'})
# df_value_counts['count']/toto
# df_value_counts.withColumn('freq', df_value_counts['count']/toto )   #   show(truncate = False) 
# df_value_counts.withColumn('freq', df_value_counts['count']/df_value_counts.agg(F.sum('count')))   #   show(truncate = False)

df_value_counts.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC # Working with columns

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Accéder à la col `District` et `Case Number`**

# COMMAND ----------

# df.columns

## Accés à une seule col  
# df.District, type(_)
# df['Case Number'], type(_)

## L'ensemble de ces écritures sont équivalentes 
# df.select('District')
# df.select('Case Number')
# df.select(df.District)
# df.select(F.col('District'))

## Accés à plusieurs col  
df.select(['District', 'Case Number'])
df.select('District', 'Case Number')

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Display only the first 5 rows of the column name IUCR**

# COMMAND ----------

df.select('IUCR').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Display only the first 4 rows of the column names Case Number, Date and Arrest**

# COMMAND ----------

df.select(['Case Number', 'Date', 'Arrest']).show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Add a column with name One, with entries all 1s [SparkByExamples](https://sparkbyexamples.com/spark/spark-add-new-column-to-dataframe/)  [stackoverflow](https://stackoverflow.com/questions/55382401/how-to-add-multiple-empty-columns-to-a-pyspark-dataframe-at-specific-locations)**

# COMMAND ----------

# df = df.withColumn('One', F.lit(1))   # `lit` pour dire `litteral` 
# df.select('District', 'One').show(5)
# df
df.One.dtype

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Remove the column One Or multiple columns**  [SparkByExamples](https://sparkbyexamples.com/pyspark/pyspark-drop-column-from-dataframe/)

# COMMAND ----------

# df = df.drop('One')
# df

## drop multiple columns 
cols = ["ID", "Case Number", "Block"]
df.drop(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with dates  [SparkByExamples](https://sparkbyexamples.com/pyspark/pyspark-to_date-convert-string-to-date-format/), [stackoverflow](https://stackoverflow.com/questions/38080748/convert-pyspark-string-to-date-format)

# COMMAND ----------

dt = spark.createDataFrame([('2019-12-25 13:30:00',)], ['Christmas'])
dt.show()
dt.dtypes


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Q : Convertir la col `Christmas`  2019-12-25 13:30:00 en `date` et en `timestamp`**

# COMMAND ----------

# Le nom de la col est trop long, il faut mettre un alias (comme en SQL)
# dt.select(F.to_date(dt.Christmas, 'yyyy-MM-dd HH:mm:ss'))  
# dt.select(F.to_date(dt.Christmas, 'yyyy-MM-dd HH:mm:ss').alias('Christmas date'))

# On sélectionne les cols : `Christmas`, `Christmas date`, `Christmas timestamp`
# dt.select([
#     dt.Christmas, 
#     F.to_date(dt.Christmas, 'yyyy-MM-dd HH:mm:ss').alias('Christmas date'), 
#     F.to_timestamp(dt.Christmas, 'yyyy-MM-dd HH:mm:ss').alias('Christmas timestamp')
#     ])# .show(truncate=False)

# Si on souhaite convertir la   
dt.withColumn('Christmas date', F.to_date(dt.Christmas, 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('Christmas timestamp', F.to_timestamp(dt.Christmas, 'yyyy-MM-dd HH:mm:ss')) 

# dt
# dt.dtypes


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Q : Convertir la col `Christmas`  25/Dec/2019 13:30:00 en `date` et en `timestamp`**

# COMMAND ----------

dt = spark.createDataFrame([('25/Dec/2019 13:30:00',)], ['Christmas'])
dt.show()
dt.dtypes


# COMMAND ----------

dt.withColumn('Christmas date', F.to_date(dt.Christmas, 'dd/MMM/yyyy HH:mm:ss')) \
    .withColumn('Christmas timestamp', F.to_timestamp(dt.Christmas, 'dd/MMM/yyyy HH:mm:ss')) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Q : Convertir la col `Christmas`  12/25/2019 01:30:00 PM en `date` et en `timestamp`  [stackoverflow](https://stackoverflow.com/questions/51680587/how-can-i-account-for-am-pm-in-string-to-datetime-conversion-in-pyspark)** 

# COMMAND ----------

# La spécificité ici est que la date comporte AM/PM
dt = spark.createDataFrame([('12/25/2019 01:30:00 PM', )], ['Christmas'])
dt.show(truncate=False)
dt.dtypes


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Q : Convertir la col `Date` du `df` et trouver combien de crimes ont été commis `12-Nov-2018` ?** 

# COMMAND ----------

# df.select(df.Date).show(truncate=False)
# df.select(df.Date).dtypes # string

## Convertir la column `Date` ? 
# df = df.withColumn('Date_', F.to_timestamp(df.Date, 'MM/dd/yyy hh:mm:ss a'))

df.select(df.Date_).dtypes # timestamp

# df

# COMMAND ----------

# On exécute la Qry sur la col `Date` (string) pr trouver combien de jours qui corresp. à `2018-11-12`
# Att. la Qry va mettre 2min et retourne 0 obs  
# df.filter(df.Date == F.lit('2018-11-12'))

# On exécute maintenant la Qry sur la col `Date_` (timestamp)
# Att. la Qry va mettre 2min et retourne 3 obs  

df.filter(df.Date_ == F.lit('2018-11-12'))


# COMMAND ----------

dt.withColumn('Christmas date', F.to_date(dt.Christmas, 'MM/dd/yyyy hh:mm:ss a')) \
  .withColumn('Christmas timestamp', F.to_timestamp(dt.Christmas, 'MM/dd/yyyy hh:mm:ss a')) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Q : What is 3 days earlier that the oldest date and 3 days later than the most recent date?**

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC # **Working with rows**

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : What are the top 10 number of reported crimes by Primary type, in descending order of occurence?**

# COMMAND ----------

# df.select('Arrest').distinct().show()
# df.select('Arrest').dtypes # boolean
(df.filter(df.Arrest == 'true').count() / df.select(df.Arrest).count())* 100

# COMMAND ----------

# MAGIC %md
# MAGIC # Challenge

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : What percentage of reported crimes resulted in an arrest?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Q : What are the top 3 locations for reported crimes?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Built-in functions

# COMMAND ----------

from pyspark.sql import functions

# COMMAND ----------

print(dir(functions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## String functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Display the Primary Type column in lower and upper characters, and the first 4 characters of the column**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Numeric functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Show the oldest date and the most recent date**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Working with joins

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Download police station data**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Q : The reported crimes dataset has only the district number. Add the district name by joining with the police station dataset**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Challenge

# COMMAND ----------

# MAGIC %md
# MAGIC **Q : What is the most frequently reported non-criminal activity?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Q : Using a bar chart, plot which day of the week has the most number of reported crime.**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Lire à partir de HDFS
# MAGIC [SparkByExamples](https://sparkbyexamples.com/spark/spark-read-write-files-from-hdfs-txt-csv-avro-parquet-json/), [saggie](https://saagie.zendesk.com/hc/en-us/articles/360029759552-PySpark-Read-and-Write-Files-from-HDFS)

# COMMAND ----------

!hdfs dfs -ls -R /

# COMMAND ----------

# Le sep est un espace
!hdfs dfs -cat /hadoop/access_log | more

# COMMAND ----------

# df_log = spark.read.csv('hdfs://localhost:8020/hadoop/access_log', sep=' ')
df_log.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ecrire dans HDFS

# COMMAND ----------

# Create data
test_write = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
test_write = spark.createDataFrame(test_write)

# Write into HDFS
test_write.write.csv("hdfs://localhost:8020/hadoop/test_write.csv", mode = 'overwrite')

# COMMAND ----------

!hdfs dfs -ls -R /
