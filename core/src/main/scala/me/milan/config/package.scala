package me.milan

import scala.util.Try

import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

package object config {

  implicit val urlConfigReader: ConfigReader[Uri] = ConfigReader[String].emap { urlAsString =>
    Uri.fromString(urlAsString).left.map { failure =>
      CannotConvert(value = urlAsString, toType = "Uri", because = failure.sanitized)
    }
  }

  implicit val autoOffsetResetConfigReader: ConfigReader[AutoOffsetReset] =
    ConfigReader[String].emap { autoOffsetAsString =>
      Try(AutoOffsetReset.valueOf(autoOffsetAsString.toUpperCase)).toEither.left.map { failure =>
        CannotConvert(value = autoOffsetAsString, toType = "AutoOffsetReset", because = failure.getMessage)
      }
    }

}
