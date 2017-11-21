package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.ConnectorResponse

import scala.concurrent.Future

trait RedshiftIsOptimized {
  self: RedshiftConnector =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    Future.successful(Right(true))
  }
}
