package squareoneinsights.redis

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import com.typesafe.config.{Config, ConfigFactory}
import redis.{RedisClient, RedisCommands, SentinelMonitoredRedisClient}
import redis.commands.Transactions

import scala.concurrent.Future

class RedisClientUtility(system: ActorSystem) {
  implicit val x = system
  implicit val executionContext: MessageDispatcher = x.dispatchers.lookup("redis-utility-dispatcher")
  val redisHost: String = "redis-master"
  val isRedisSentinelOn: Boolean =  false
  val redisAuth: String = "password"
  val redis: Transactions with RedisCommands = if (isRedisSentinelOn) {
    val redisSentinelMaster = "mymaster"
    val arrayOfHostIp = redisHost.split(",")
    val seqOfIpTuple: Seq[(String, Int)] = arrayOfHostIp
      .map(q => q.split(" "))
      .map { case Array(hostIp, portIp) =>
        (hostIp, portIp.toInt)
      }
      .toSeq

    SentinelMonitoredRedisClient(seqOfIpTuple, redisSentinelMaster, Some(redisAuth))
  } else RedisClient(redisHost, password = Some(redisAuth))


  def insertCityRecordIntoRedis(cityData: CityRecords): Future[Long] = {
    import cityData._
    redis.geoAdd("location:ca", lat, lng, city.getOrElse(""))
  }


}
