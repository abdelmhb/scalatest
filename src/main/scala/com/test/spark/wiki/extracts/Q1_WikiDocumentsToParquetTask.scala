package com.test.spark.wiki.extracts

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import java.io.FileInputStream
import java.io.InputStream
import org.apache.spark.SparkContext

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    val sc : SparkContext = session.sparkContext
    
      // TODO Q1 Transformer cette seq en dataset
    sc.parallelize(getLeagues, 2)
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veiller à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage
            val doc = Jsoup.connect(url).get()
            val elt = doc.select("#mw-content-text > div.mw-parser-output > table.wikitable.gauche").get(0)
      			val tableArray : Array<LeagueStanding> = ();
			
      			for (Element tr : elt.select("tr")) {
      			  Element tdPosition : tr.select("td").get(0)
      			  Element tdTeam : tr.select("td").get(0)
      			  Element tdPoints : tr.select("td").get(0)
      			  played : tr.select("td").get(0).html()
      			  won : tr.select("td").get(0).html()
      			  drawn : tr.select("td").get(0).html()
      			  lost : tr.select("td").get(0).html()
      			  goalsFor : tr.select("td").get(0).html()
      			  goalsAgainst : tr.select("td").get(0).html()
      			  goalDifference : tr.select("td").get(0).html()
      			  // Recherche du texte encaplsulé souvent dans autres balises imbriquées
    					while (tdPosition.childrenSize()>0) {
    						tdPosition = tdPosition.child(0);
    					}
    					while (tdTeam.childrenSize()>0) {
    						tdTeam = tdTeam.child(0);
    					}
    					while (tdPoints.childrenSize()>0) {
    						tdPoints = tdPoints.child(0);
    					}

    					position = tdPosition.html().replace(".", "")
    					team = tdTeam.html()
    					points = tdPoints.html()
    					
    					
    					leagueStanding : LeagueStanding = new LeagueStanding(
    					                          league.toInt , season.toInt 
    					                          position.toInt , team, points.toInt ,
    					                          played.toInt , won.toInt , drawn.toInt , lost.toInt 
    					                          goalsFor.toInt , goalsAgainst.toInt , goalDifference.toInt 
    					                        )
    					tableArray = tableArray ++ Array( leagueStanding )  
      			}
            return tableArray
            
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              // ils sont affichés dans le repertoire /mnt/var/log/application
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
      // Le format parquet est un format optimisé pour les traitements des grands fichiers
      // sous hadoop

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
      // Bénéficier des avantages de Spark en calcul distribué, de la tolérance aux pannes
      // et de la force du moteur Catalyst 
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    val inputStream : InputStream = new FileInputStream("leagues.yaml")
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@BeanProperty name: String,
                       @BeanProperty url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
