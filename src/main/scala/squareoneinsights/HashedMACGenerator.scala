package squareoneinsights

import cats.syntax.either._
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.util.Try

class HashedMACGenerator {

  private val logger        = LoggerFactory.getLogger(classOf[HashedMACGenerator])



  private def hmacSHA256(hashingSecret: String): Either[String, Mac] = {
    val secretKey: SecretKeySpec = new SecretKeySpec(hashingSecret.getBytes,"HmacSHA256")
    val mac = Try(Mac.getInstance("HmacSHA256")).toEither.leftMap(_.toString)
    mac.bimap(
      exception => logger.error(s"Unable to initialize Hashed MAC generator due wrong algorithm being used $exception"),
      mac => {
        mac.init(secretKey)
      }
    )
    mac
  }

  def getHMAC(message: String, hashingSecret: String): Either[String, String] = {
    val hashedMessage = hmacSHA256(hashingSecret).map(mac => java.util.Base64.getEncoder.encodeToString(mac.doFinal(message.getBytes)))
    hashedMessage.bimap(
      exception => logger.error(s"Failed to hash value due to exception: $exception"),
      hashedMsg => logger.info(s"Successfully hashed message to $hashedMsg")
    )
    hashedMessage
  }
}
