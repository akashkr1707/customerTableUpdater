package squareoneinsights 

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.github.tototoshi.csv._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object CustomerTableUpdater {
  def main(args: Array[String]): Unit = {

    

    implicit val system: ActorSystem = ActorSystem("csUpdateActor")
    val executionContext: ExecutionContext = system.dispatcher

    if (args.length < 3) {
      println(s"Please provide the required argument")
      system.terminate()
    }

    if (args.length < 4) {
      println(s"Please provide the required argument")
      system.terminate()
    }

    val paraallalism = args(0).toInt
    val hashingSecret = args(1).toString
    val inputFile = args(2).toString
    val outputFile = args(3).toString
    val hashedMACGenerator = new HashedMACGenerator
    val reader = CSVReader.open(inputFile)
    val writer = CSVWriter.open(outputFile)

    // Reading CSV file and parsing to case class

    def csvRecords = {
      val rescord = reader.allWithHeaders().map { row =>
        CsvRecord(
          partner_id = Some(row("partner_id")), cif = Some(row("cif")), aadhar_detail = Some(row("aadhar_detail")), aadhar_hash = Some(row("aadhar_hash")), aadhar_id_flag = Some(row("aadhar_id_flag")), aadhar_mask = Some(row("aadhar_mask")), address_1 = Some(row("address_1")), address_2 = Some(row("address_2")), address_3 = Some(row("address_3")), alt_mobile_number = Some(row("alt_mobile_number")), attribute_1 = Some(row("attribute_1")), attribute_2 = Some(row("attribute_2")), attribute_3 = Some(row("attribute_3")),
          attribute_4 = Some(row("attribute_4")), attribute_5 = Some(row("attribute_5")), birth_date = Some(row("birth_date")), category = Some(row("category")), city = Some(row("city")), ckyc_number = Some(row("ckyc_number")), country = Some(row("country")), created_timestamp = Some(row("created_timestamp")),
          customer_risk_level = Some(row("customer_risk_level")), customer_type = Some(row("customer_type")), declaration_ckyc_number = Some(row("declaration_ckyc_number")), declaration_last_name = Some(row("declaration_last_name")), declaration_pan = Some(row("declaration_pan")),
          din_dpin = Some(row("din_dpin")), driving_license_number = Some(row("driving_license_number")), email = Some(row("email")), employer_district = Some(row("employer_district")), employer_locality = Some(row("employer_locality")), employer_name = Some(row("employer_name")), father_name = Some(row("father_name")),
          gender = Some(row("gender")), income_range = Some(row("income_range")), is_married = Some(row("is_married")), is_staff = Some(row("is_staff")), is_urban = Some(row("is_urban")), last_kyc_date = Some(row("last_kyc_date")), last_name = Some(row("last_kyc_date")), latitude = Some(row("latitude")), loadmoney_card_count = Some(row("loadmoney_card_count")),
          longitude = Some(row("longitude")), loop_type = Some(row("loop_type")), measure_1 = Some(row("measure_1")), measure_2 = Some(row("measure_2")), measure_3 = Some(row("measure_3")), measure_4 = Some(row("measure_4")), Some(row("measure_5")), middle_name = Some(row("middle_name")), mobile_number = Some(row("mobile_number")), mother_name = Some(row("mother_name")),
          name = Some(row("name")), nationality = Some(row("nationality")), non_resident = Some(row("non_resident")), npr = Some(row("npr")), nrega_card = Some(row("nrega_card")), occupation = Some(row("occupation")), other_customer_type = Some(row("other_customer_type")), other_occupation = Some(row("other_occupation")), pan_detail = Some(row("pan_detail")), pan_hash = Some(row("pan_hash"))
          , pan_mask = Some(row("pan_mask")), passport_number = Some(row("passport_number")), pekrn = Some(row("pekrn")), pin_code = Some(row("pin_code")), sec_address1 = Some(row("sec_address1")), sec_city = Some(row("sec_city")), sec_country = Some(row("sec_country")), sec_district = Some(row("sec_district")),
          sec_locality = Some(row("sec_locality")), sec_pincode = Some(row("sec_pincode")), sec_state = Some(row("sec_state")), spouse_name = Some(row("spouse_name")), state = Some(row("state")), telephone_number = Some(row("telephone_number")), time_1 = Some(row("time_1")), time_2 = Some(row("time_2")), time_3 = Some(row("time_3")), time_4 = Some(row("time_4")), time_5 = Some(row("time_5")),
          ucic = Some(row("ucic")), updated_at = Some(row("updated_at")), updated_timestamp = Some(row("updated_timestamp")), voter_id = Some(row("voter_id")), work_address = Some(row("work_address")), work_city = Some(row("work_city")), work_country = Some(row("work_country")), work_latitude = Some(row("work_latitude")), work_longitude = Some(row("work_longitude")),
          work_pin_code = Some(row("work_pin_code")), work_state = Some(row("work_state"))
        )
      }
      println(s"No of record processed${rescord.length}")
      rescord
    }


    lazy val convertStringToMasked: Option[String] => Option[String] = {
      case Some(str) => Some(str.replaceAll("\\w(?=(.{4}(\\w|$)))", "*"))
      case None => None
    }

    def getHashedValue(value: Option[String], hashedMACGenerator: HashedMACGenerator): Option[String] = {
      value match {
        case Some(data) => hashedMACGenerator.getHMAC(data,hashingSecret) match {
          case Right(value) => Some(value)
          case Left(str) => None
        }
        case None => None
      }
    }

    // Manipulate the data (update fields, etc.)
    def updatedRecords: Flow[List[CsvRecord], List[CsvRecord], NotUsed] = Flow[List[CsvRecord]].mapAsyncUnordered(paraallalism) { csvRecords =>
      Future {
        csvRecords.map { record =>
          val pan_maskData = convertStringToMasked(record.pan_detail)
          val aadhar_hashData = getHashedValue(record.aadhar_detail, hashedMACGenerator)
          val aadhar_maskData = convertStringToMasked(record.aadhar_detail)
          val pan_hashData = getHashedValue(record.pan_detail, hashedMACGenerator)
          record.copy(pan_mask = pan_maskData, aadhar_mask = aadhar_maskData, pan_hash = pan_hashData, aadhar_hash = aadhar_hashData)
        }
      }
    }

    // Writing updated records to CSV file

    def writerCsv: Flow[List[CsvRecord], Done.type, NotUsed] = Flow[List[CsvRecord]].mapAsyncUnordered(paraallalism) { updatedRecords =>
      Future {
        writer.writeRow(List("partner_id", "cif", "aadhar_detail", "aadhar_hash", "aadhar_id_flag", "aadhar_mask", "address_1", "address_2", "address_3", "alt_mobile_number", "attribute_1", "attribute_2", "attribute_3",
          "attribute_4", "attribute_5", "birth_date", "category", "city", "ckyc_number", "country", "created_timestamp",
          "customer_risk_level", "customer_type", "declaration_ckyc_number", "declaration_last_name", "declaration_pan",
          "din_dpin", "driving_license_number", "email", "employer_district", "employer_locality", "employer_name", "father_name",
          "gender", "income_range", "is_married", "is_staff", "is_urban", "last_kyc_date", "last_name", "latitude", "loadmoney_card_count",
          "longitude", "loop_type", "measure_1", "measure_2", "measure_3", "measure_4", "measure_5", "middle_name", "mobile_number", "mother_name",
          "name", "nationality", "non_resident", "npr", "nrega_card", "occupation", "other_customer_type", "other_occupation", "pan_detail", "pan_hash", "pan_mask", "passport_number", "pekrn", "pin_code", "sec_address1", "sec_city", "sec_country", "sec_district",
          "sec_locality", "sec_pincode", "sec_state", "spouse_name", "state", "telephone_number", "time_1", "time_2", "time_3", "time_4", "time_5",
          "ucic", "updated_at", "updated_timestamp", "voter_id", "work_address", "work_city", "work_country", "work_latitude", "work_longitude",
          "work_pin_code", "work_state"
        )) // Write header
        updatedRecords.foreach { record: CsvRecord => {
          import record._
          writer.writeRow(List(
            partner_id.getOrElse(""), cif.getOrElse(""), aadhar_detail.getOrElse(""), aadhar_hash.getOrElse(""), aadhar_id_flag.getOrElse(""), aadhar_mask.getOrElse(""), address_1.getOrElse(""), address_2.getOrElse(""), address_3.getOrElse(""), alt_mobile_number.getOrElse(""), attribute_1.getOrElse(""), attribute_2.getOrElse(""), attribute_3.getOrElse(""),
            attribute_4.getOrElse(""), attribute_5.getOrElse(""), birth_date.getOrElse(""), category.getOrElse(""), city.getOrElse(""), ckyc_number.getOrElse(""), country.getOrElse(""), created_timestamp.getOrElse(""),
            customer_risk_level.getOrElse(""), customer_type.getOrElse(""), declaration_ckyc_number.getOrElse(""), declaration_last_name.getOrElse(""), declaration_pan.getOrElse(""),
            din_dpin.getOrElse(""), driving_license_number.getOrElse(""), email.getOrElse(""), employer_district.getOrElse(""), employer_locality.getOrElse(""), employer_name.getOrElse(""), father_name.getOrElse(""),
            gender.getOrElse(""), income_range.getOrElse(""), is_married.getOrElse(""), is_staff.getOrElse(""), is_urban.getOrElse(""), last_kyc_date.getOrElse(""), last_name.getOrElse(""), latitude.getOrElse(""), loadmoney_card_count.getOrElse(""),
            longitude.getOrElse(""), loop_type.getOrElse(""), measure_1.getOrElse(""), measure_2.getOrElse(""), measure_3.getOrElse(""), measure_4.getOrElse(""), measure_5.getOrElse(""), middle_name.getOrElse(""), mobile_number.getOrElse(""), mother_name.getOrElse(""),
            name.getOrElse(""), nationality.getOrElse(""), non_resident.getOrElse(""), npr.getOrElse(""), nrega_card.getOrElse(""), occupation.getOrElse(""), other_customer_type.getOrElse(""), other_occupation.getOrElse(""), pan_detail.getOrElse(""), pan_hash.getOrElse(""), pan_mask.getOrElse(""), passport_number.getOrElse(""), pekrn.getOrElse(""), pin_code.getOrElse(""), sec_address1.getOrElse(""), sec_city.getOrElse(""), sec_country.getOrElse(""), sec_district.getOrElse(""),
            sec_locality.getOrElse(""), sec_pincode.getOrElse(""), sec_state.getOrElse(""), spouse_name.getOrElse(""), state.getOrElse(""), telephone_number.getOrElse(""), time_1.getOrElse(""), time_2.getOrElse(""), time_3.getOrElse(""), time_4.getOrElse(""), time_5.getOrElse(""),
            ucic.getOrElse(""), updated_at.getOrElse(""), updated_timestamp.getOrElse(""), voter_id.getOrElse(""), work_address.getOrElse(""), work_city.getOrElse(""), work_country.getOrElse(""), work_latitude.getOrElse(""), work_longitude.getOrElse(""),
            work_pin_code.getOrElse(""), work_state.getOrElse("")
          ))
        }
        }
        Done
      }
    }

    Source.single(csvRecords).via(updatedRecords).via(writerCsv).runWith(Sink.ignore).onComplete {
      case Success(value) => println("CSV file successfully updated and copied.")
        reader.close()
        writer.close()
        system.terminate()
      case Failure(err) => reader.close()
        writer.close()
        println("Failed to updated and copied csv.")
        throw new RuntimeException("Failed to updated and copied csv.")
        system.terminate()
    }
  }
}
