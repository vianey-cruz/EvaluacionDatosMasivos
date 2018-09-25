////1////Comienza una simple Spark Session
import org.apache.spark.sql.SparkSession
val exa = SparkSession.builder().getOrCreate()
////2////Cargue el archivo Netflix
val vc= spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")
vc.show()
////3////Nombres de las columnas
vc.columns
////4////Como es el esquema
vc.printSchema()
////5////Imprimir las primeras 5 Columnas
vc.select($"Date",$"Open",$"High",$"Low",$"Close").show(5)
////6////Usa describe para aprender sobre el DataFrame
vc.describe().show()
////7////Crea nuevo dataframe con una columna llamada HV ratio que sera la relacion entre el precio alto frente
/////////al volumen de acciones negociadas por un dia.
val vc2=vc.withColumn("HV Ratio",vc("High")+vc("volumen"))
vc2.columns
////8////Que dia tuvo Peak High en Price
vc.orderBy($"High".desc).show(5)
////9////Cual es el significado de la columna close
vc.select(mean("Close")).show()
////10////Cual es el maximo y minimo de la columna volumen
vc.select(max("Volume")).show()
vc.select(min("Volume")).show()
////11////Con sintaxis Scala/Spark $ conteste lo siguiente
  ///a////cuantos dias fue el cierre inferior a 600
  vc.filter($"Close"<600).count()
  ///b////Que porcentaje del tiempo fue el alto mayor de 500
  (vc.filter($"High" > 500).count() * 1.0/ vc.count())*100
  ///c////Cual es la correlacion de Pearson entre alto y volumen
  vc.select(corr("High","Volume")).show()
  ///d////Cual es el maximo alto por ao
  val yvc = df.withColumn("Year",year(df("Date")))
  val ymax = yvc.select($"Year",$"High").groupBy("Year").max()
  val res = ymax.select($"Year",$"max(High)")
  res.orderBy("Year").show()
  ///e////Cual es el promedio de cierre por cada mes del calendario
  val mdf = df.withColumn("Month",month(vc("Date")))
  val mavgs = mdf.select($"Month",$"Close").groupBy("Month").mean()
  mavgs.select($"Month",$"avg(Close)").orderBy("Month").show()
