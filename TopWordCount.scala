package com.sparkweek6.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object TopWordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("TopWordCount").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named WordCountBetterSorted
    //alternative: val sc = new SparkContext("local[*]", "WordCount_v3")
    val sc = new SparkContext(conf)  
    
    // Load each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word - to be able to do the counting we added one 
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey(false)
    val res = wordCountsSorted.collect()
    val topTen = res.take(20)
    
    // Exclude list 
    val excludeList = List("you","to","your","the","a","of","and", "are", "for", "if")
    // Print the results, flipping the (count, word) results to word: count as we go.
    
    
    for (result <- topTen) {
      if (!(excludeList.contains(result._2))){
        val count = result._1
        val word = result._2 
        println(s"$word: $count")
     }
    }
          
    
  }
  
}

