package com.sparkweek6.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/** Find the movies with the most ratings. */
object PopularMovies_v2 {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies_v2").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named PopularMovies_v2
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies_v2")
    val sc = new SparkContext(conf) 

    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)

    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey((x, y) => x + y)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sort
    val sortedMovies = flipped.sortByKey(false)

    // Fold in the movie names from the broadcast variable - value of movie id is the x 
    val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1)).filter(x => x._2 > 200)

    // Collect and print results
    val results = sortedMoviesWithNames.collect()
     val topTen = results.take(10)

    topTen.foreach(println)
  }

}

