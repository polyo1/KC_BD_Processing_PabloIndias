package Examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col, expr, round, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  def ejercicio1(estudiantes: DataFrame)(spark: SparkSession): DataFrame = {
    // Mostramos el esquema del DataFrame
    println("Esquema del DataFrame:")
    estudiantes.printSchema()

    // Filtramos los estudiantes con calificación mayor a 8
    val filtradosDF = estudiantes.filter(col("calificacion") > 8)

    // Seleccionamos los nombres y calificaciones, ordenados por calificacion descendente
    val nombresOrdenadosDF = filtradosDF
      .select(col("nombre"), col("calificacion"))
      .orderBy(col("calificacion").desc)

    // Devolvemos el DataFrame resultante
    nombresOrdenadosDF
  }


  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {

    // Definimos la UDF para comprobar si un numero es par o impar
    val esParImparUDF = udf((numero: Int) => if (numero % 2 == 0) "Par" else "Impar")

    // Ejecutamos la UDF en la columna del DataFrame
    val resultadoDF = numeros.withColumn("tipo", esParImparUDF(col("numero")))

    // Devolvemos el DataFrame resultante
    resultadoDF
  }




  /**Ejercicio 3: Joins y agregaciones
  Pregunta: Dado dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  */

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    // Hacemos un join entre los dos DataFrames
    val estudiantesConCalificaciones = estudiantes.join(calificaciones, "ID")

    // Calculamos la media de calificaciones por estudiante
    val promedioCalificaciones = estudiantesConCalificaciones
      .groupBy(col("ID"), col("Nombre"))
      .agg(round(avg("Calificacion"),2).as("Media"))

    // Devolvemos el DataFrame resultante
    promedioCalificaciones
  }



  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

  */

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {

    // Crear un RDD a partir de la lista de palabras
    val palabrasRDD: RDD[String] = spark.sparkContext.parallelize(palabras)

    // Mapear cada palabra a un par (palabra, 1) y luego reducir por clave para contar las ocurrencias
    val ocurrenciasRDD: RDD[(String, Int)] = palabrasRDD
      .map(palabra => (palabra, 1)) // Crear pares (palabra, 1)
      .reduceByKey(_ + _)           // Sumar los valores para cada palabra

    // Devolver el RDD resultante
    ocurrenciasRDD

  }


  /**
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */
  def ejercicio5(ventas: DataFrame)(spark: SparkSession): DataFrame = {

    // Calculamos el ingreso total con el csv que leemos

    val ingresoTotal: DataFrame = ventas.groupBy("id_venta", "id_producto").agg(sum(expr("cantidad * precio_unitario")).as("ingreso_total"))

    // Devolvemos el DataFrame resultante
    ingresoTotal
  }
}

