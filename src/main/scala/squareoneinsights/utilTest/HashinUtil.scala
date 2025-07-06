package squareoneinsights.utilTest

import squareoneinsights.HashedMACGenerator

object HashinUtil extends App{
  val hashedMACGenerator = new HashedMACGenerator()


  def hashStringValue(value: String): Option[String] = {
    import cats.implicits._
    val res = hashedMACGenerator.getHMAC(value, "\uFFFF") match {
      case Right(data) => Right(data)
      case Left(str) => Left(str)
    }
    res.toOption
  }

  println(hashStringValue("\uFFFF"))
}
