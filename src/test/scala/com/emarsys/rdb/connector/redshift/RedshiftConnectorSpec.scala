package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.redshift.RedshiftConnector.RedshiftConnectionConfig
import org.scalatest.{Matchers, WordSpecLike}

class RedshiftConnectorSpec extends WordSpecLike with Matchers{

  "RedshiftConnectorTest" when {

    "#createUrl" should {

      val exampleConnection = RedshiftConnectionConfig(
        host = "host",
        port = 123,
        dbName = "database",
        dbUser = "me",
        dbPassword = "secret",
        connectionParams = "?param1=asd"
      )

      "creates url from config" in {
        RedshiftConnector.createUrl(exampleConnection) shouldBe "jdbc:redshift://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        RedshiftConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:redshift://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        RedshiftConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:redshift://host:123/database"
      }


    }

    "#checkSsl" should {

      "return true if empty connection params" in {
        RedshiftConnector.checkSsl("") shouldBe true
      }

      "return true if not contains ssl=false" in {
        RedshiftConnector.checkSsl("?param1=param&param2=param2") shouldBe true
      }

      "return false if contains ssl=false" in {
        RedshiftConnector.checkSsl("?param1=param&ssl=false&param2=param2") shouldBe false
      }

    }

  }
}
