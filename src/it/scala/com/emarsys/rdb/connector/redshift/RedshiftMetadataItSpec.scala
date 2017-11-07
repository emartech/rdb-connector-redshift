package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.redshift.utils.TestHelper
import com.emarsys.rdb.connector.test.MetadataItSpec
import slick.util.AsyncExecutor

import concurrent.duration._
import scala.concurrent.Await
import concurrent.ExecutionContext.Implicits.global

class RedshiftMetadataItSpec extends MetadataItSpec {

  val connector: Connector = Await.result(RedshiftConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds).right.get

  def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE "$tableName" (
                            |    PersonID int,
                            |    LastName varchar(255),
                            |    FirstName varchar(255),
                            |    Address varchar(255),
                            |    City varchar(255)
                            |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$tableName";""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), 5.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql = s"""DROP VIEW "$viewName";"""
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropViewSql)
      _ <- TestHelper.executeQuery(dropTableSql)
    } yield (), 5.seconds)
  }

}
