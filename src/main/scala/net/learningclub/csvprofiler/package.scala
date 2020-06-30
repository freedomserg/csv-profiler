package net.learningclub

import io.circe._
import io.circe.generic.semiauto._

package object csvprofiler {

  object SupportedDataTypes extends Enumeration {
    val INTEGER = Value("integer")
    val STRING = Value("string")
    val DATE = Value("date")
    val BOOLEAN = Value("boolean")
  }

  case class FieldProfile(
                           column: String,
                           unique_values: Int,
                           values: Map[String, Int]
                         )

  object FieldProfile {
    implicit val profileDecoder: Decoder[FieldProfile] = deriveDecoder[FieldProfile]
    implicit val profileEncoder: Encoder[FieldProfile] = deriveEncoder[FieldProfile]
  }

}
