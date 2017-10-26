package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.redshift.utils.ConfigHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.{Await, Future}
import concurrent.duration._

class RedshiftConnectorItSpec extends WordSpecLike with Matchers {
  "RedshiftConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val executor = AsyncExecutor.default()

    "#testConnection" should {

      "return ok in happy case" in {
        val connection = Await.result(RedshiftConnector(ConfigHelper.dbConnectorConfig)(executor), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Right()
        connection.close()
      }

      "return error if cant connect" in {
        val badConnection = ConfigHelper.dbConnectorConfig.copy(host = "asd.asd.asd")
        val connection = Await.result(RedshiftConnector(badConnection)(executor), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

    }
  }
}
