package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnat" +
      "par décennie")

  }
}
