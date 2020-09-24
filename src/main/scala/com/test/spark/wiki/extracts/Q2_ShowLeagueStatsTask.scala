package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.api.java.UDF1

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
      val moyenneButChampionnat = standings.groupBy(col("league"), col("season")).agg(avg("goalsFor").as("MoyenneButs"))

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    val equipePlusTitree = standings.filter(col("rang").equalTo(lit(1))
                                              .and(col("league").equalTo(lit("Ligue 1"))))
                                              .groupBy(col("team")).agg(max(count(lit(1))).as("NombreTitres"))

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    val moyennePointsVainqueur = standings.filter(col("rang").equalTo(lit(1)))
                                            .groupBy(col("league"))
                                            .agg(avg(col("points")).as("MoyennePointsVainqueur"))
    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val getDecade: Int => String = _.toString().substring(0, 3) + "X"
    val decadeUDF = udf(getDecade)
    
    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnat" +
      "par décennie")
      
    val moyenEcarts = standings.withColumn("decade", decadeUDF(col("season")))
                                .groupBy(col("league"), col("decade"))
                                .agg(avg(when(col("position").equalTo(lit(1)), col("points")))
                                    .minus(avg(when(col("position").equalTo(lit(10)), col("points")))))

  }
}
