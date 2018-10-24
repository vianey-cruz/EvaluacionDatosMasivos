import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

// se carga y se analizan los datos y se convierte en un DataFrame
val data = MLUtils.loadLibSVMFile(sc, "dataset.txt")
// // Se dividen los datos en conjuntos de prueba y entrenamiento
//  (70% para el entrenamiento y 30% para la prueba)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1)) //

// Se entrena el modelo de Bosque Aleatorio
// caracter. vacias //categoricalFeaturesInfo indica que todas las categorias son continuas
val numClasses = 2 // se le asigna el numero de clases
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 4 // se le asigna el numero de arboles
val featureSubsetStrategy = "auto" // Se utiliza para aumentar la velocidad del entrenamiento
val impurity = "gini" // Impureza // usa este criterio "Gini" para hacer la division de ramas.
val maxDepth = 4 // profundidad
val maxBins = 32 // contenedores para las carac continuas

// se le asignan los parametro de entrada requeridos
val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// se evalua el modelo en las instancias de prueba y calcula el error de prediccion
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

// filtra el resultado con mayor numero de votos para no mostrar todos los datos.
val testEr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println(s"Test Error = $testErr")
println(s"Learned classification forest model:\n ${model.toDebugString}")

// Guarda y carga el modelo
model.save(sc, "target/tmp/myRandomForestClassificationModella")
val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModella")
