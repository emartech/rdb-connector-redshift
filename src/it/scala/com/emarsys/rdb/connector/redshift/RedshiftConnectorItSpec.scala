package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError}
import com.emarsys.rdb.connector.redshift.utils.TestHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftConnectorItSpec extends WordSpecLike with Matchers {
  "RedshiftConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val executor                  = AsyncExecutor.default()
    val timeout                   = 8.seconds

    "create connector" should {

      "return error if not use ssl" in {
        var connectionParams = TestHelper.TEST_CONNECTION_CONFIG.connectionParams
        if (!connectionParams.isEmpty) {
          connectionParams += "&"
        }
        connectionParams += "ssl=false"

        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = connectionParams)
        val connection    = Await.result(RedshiftConnector(badConnection)(executor), timeout)
        connection shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect ok" in {

        val connectorEither =
          Await.result(RedshiftConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), timeout)

        connectorEither shouldBe a[Right[_, _]]
      }

    }

    "#testConnection" should {

      "return ok in happy case" in {
        val connection =
          Await.result(RedshiftConnector(TestHelper.TEST_CONNECTION_CONFIG)(executor), timeout).toOption.get
        val result = Await.result(connection.testConnection(), timeout)
        result shouldBe Right()
        connection.close()
      }

      "return error if cant connect" in {
        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(host = "asd.asd.asd")
        val connection    = Await.result(RedshiftConnector(badConnection)(executor), timeout)
        connection shouldBe a[Left[_, _]]
        connection.left.get shouldBe a[ConnectionError]
      }

    }
  }
}
