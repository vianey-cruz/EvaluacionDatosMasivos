// 1
import org.apache.spark.sql.SparkSession

// 2
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// 3
val spark = SparkSession.builder().getOrCreate()

// 4
import org.apache.spark.ml.clustering.KMeans

// 5
val dataset = spark.read.option("header","true").option("inferSchema","true").csv("Wholesale customers data.csv")

// 6
val feature_data = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicassen")

// 7
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors


// 8
val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")

// 9
val training_data = assembler.transform(feature_data)

// 10
val model = kmeans.fit(training_data)
val kmeans = new KMeans().setK(3).setSeed(1L)

// 11
val WSSSE = model.computeCost(training_data)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// 12
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
