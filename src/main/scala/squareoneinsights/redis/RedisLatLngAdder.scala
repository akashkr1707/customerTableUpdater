package squareoneinsights.redis

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import cats.syntax.writer
import com.github.tototoshi.csv.{CSVReader, CSVWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


object RedisLatLngAdder {
  def main(args: Array[String]): Unit = {


    implicit val system: ActorSystem = ActorSystem("redisLatLngAdderActor")
    val executionContext: ExecutionContext = system.dispatcher

    val redisClientUtility =  new RedisClientUtility(system)


    val reader = CSVReader.open("cityData.csv")

    // Reading CSV file and parsing to case class

    def cityRecords = {
      val record = reader.allWithHeaders().map { row =>
        CityRecords(city = Some(row("city")), lat = getDefaultDoubleValue(Some(row("lat"))), lng = getDefaultDoubleValue(Some(row("lng"))))
      }
      println(s"No of record processed${record.length}")
      record
    }

    def getDefaultDoubleValue(str: Option[String]): Double =
      str.getOrElse("0.0").toDouble

    val insertCityDataIntoRedis: Flow[List[CityRecords], Done.type, NotUsed] =  Flow[List[CityRecords]].map{ record =>
      record.foreach{
        cityData =>  redisClientUtility.insertCityRecordIntoRedis(cityData)
      }
     Done
    }

    Source.single(cityRecords).via(insertCityDataIntoRedis).runWith(Sink.ignore).onComplete {
      case Success(value) => println("City Data successfully inserted.")
        reader.close()
        system.terminate()
      case Failure(err) => reader.close()
        println("Failed to updated and copied csv.")
        throw new RuntimeException("Failed to updated and copied csv.")
        system.terminate()
    }
  }


}
