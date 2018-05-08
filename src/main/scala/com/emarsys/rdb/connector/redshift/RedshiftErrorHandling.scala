package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError}

trait RedshiftErrorHandling {
  self: RedshiftConnector =>
  protected def errorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] = {
    case ex: Exception => Left(ConnectionError(ex))
  }
}
