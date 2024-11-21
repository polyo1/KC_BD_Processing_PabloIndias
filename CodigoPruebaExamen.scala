package Examen

import Examen.examen.{ejercicio1, ejercicio2, ejercicio3, ejercicio4, ejercicio5}
import org.apache.spark.sql.DataFrame
import utils.TestInit

class CodigoPruebaExamen extends TestInit{
  import spark.implicits._

  //Ejercicio 1 --------------------------------------------------------

  val estudiantesDF = Seq(
    ("Ana", 20, 9.5),
    ("Luis", 22, 7.8),
    ("Carlos", 19, 8.9),
    ("Marta", 21, 6.4),
    ("Sofia", 20, 9.8),
    ("Pablo", 23, 8.2)
  ).toDF("nombre", "edad", "calificacion")


  "ejercicio1" should "Ordenar califiaciones de un DF dado" in {

    val resultadoDF = ejercicio1(estudiantesDF)(spark)

    resultadoDF.show

  }

  //Ejercicio 2 --------------------------------------------------------

  val numerosDF = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")

  "ejercicio2" should "Crear un DF que diga si cada número de otro DF es par o impar" in {

    val resultadoDF = ejercicio2(numerosDF)(spark)

    resultadoDF.show()
  }
  //Ejercicio 3 --------------------------------------------------------

  "ejercicio3" should "Crear un DF que muestre las medias de las calificaciones uniendo dos DF, una con nombres y otra con calificaciones" in {

    val nombresID = Seq(
      (1, "Pablo"),
      (2, "Silvia"),
      (3, "Alvaro"),
      (4, "Ana"),
      (5, "Javier"),
      (6, "Alonso"),
      (7, "Maria"),
      (8, "Elena"),
      (9, "Mamen")
    ).toDF("ID", "Nombre")
    val asignaturasCalificaciones = Seq(
      (1, "Matematicas", 9.5),
      (1, "Historia", 8.0),
      (2, "Matematicas", 7.8),
      (2, "Historia", 8.9),
      (3, "Matematicas", 6.4),
      (3, "Historia", 7.2),
      (4, "Matematicas", 9.0),
      (4, "Historia", 8.5),
      (5, "Matematicas", 5.5),
      (5, "Historia", 6.8),
      (6, "Matematicas", 7.0),
      (6, "Historia", 7.5),
      (7, "Matematicas", 8.5),
      (7, "Historia", 9.2),
      (8, "Matematicas", 6.0),
      (8, "Historia", 6.5),
      (9, "Matematicas", 8.0),
      (9, "Historia", 7.8)
    ).toDF("ID", "Asignatura", "Calificacion")

   val media = ejercicio3(nombresID, asignaturasCalificaciones)

    media.show

  }

  //Ejercicio 4 --------------------------------------------------------
  "ejercicio4" should "Contar las palabras de una cadena y mostrar las repeticiones en una lsita de palabras" in {

    val palabras = List("rojo", "azul", "azul", "verde", "rojo", "rojo", "verde", "azul")

    val resultadoRDD = ejercicio4(palabras)(spark)


    resultadoRDD.collect().foreach { case (palabra, cuenta) =>
      println(s"$palabra: $cuenta")

    }
  }
  //Ejercicio 5 --------------------------------------------------------

  "ejercicio5" should "Calcula el ingreso total de un csv cargado" in {

    //Leemos el archivo csv y lo importamos en la variable path

    val ventas: DataFrame = spark.read.option("header", true)
      .csv("C:/Users/User/Documents/Bootcamp/Big Data Processing/Big-Data-Processing/src/test/resources/examen/ventas.csv")

    //Ejecutamos el ejercicio 5 con el DataFrame creado

    val resultado = ejercicio5(ventas)(spark)

    resultado.show()
  }
}
