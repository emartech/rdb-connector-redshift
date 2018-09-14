package com.emarsys.rdb.connector.redshift

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.DefaultSqlWriters._

import scala.concurrent.duration.FiniteDuration

trait RedshiftSimpleSelect extends RedshiftStreamingQuery {
  self: RedshiftConnector =>

  override def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(timeout)(select.toSql)
  }
}
