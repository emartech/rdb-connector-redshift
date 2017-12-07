package com.emarsys.rdb.connector.redshift

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector, ConnectorCompanion, MetaData}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.redshift.RedshiftConnector.RedshiftConnectorConfig
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class RedshiftConnector(
                         protected val db: Database,
                         protected val connectorConfig: RedshiftConnectorConfig
                       )(
                         implicit val executionContext: ExecutionContext
                       )
  extends Connector
    with RedshiftTestConnection
    with RedshiftMetadata
    with RedshiftSimpleSelect
    with RedshiftRawSelect
    with RedshiftIsOptimized
    with RedshiftRawDataManipulation {

  override def close(): Future[Unit] = {
    db.shutdown
  }
}

object RedshiftConnector extends ConnectorCompanion {

  case class RedshiftConnectionConfig(
                                       host: String,
                                       port: Int,
                                       dbName: String,
                                       dbUser: String,
                                       dbPassword: String,
                                       connectionParams: String
                                     ) extends ConnectionConfig

  case class RedshiftConnectorConfig(
                                      queryTimeout: FiniteDuration,
                                      streamChunkSize: Int
                                    )

  private val defaultConfig = RedshiftConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  def apply(
             config: RedshiftConnectionConfig,
             connectorConfig: RedshiftConnectorConfig = defaultConfig
           )(
             executor: AsyncExecutor
           )(
             implicit executionContext: ExecutionContext
           ): ConnectorResponse[RedshiftConnector] = {

    if (checkSsl(config.connectionParams)) {

      val db = Database.forURL(
        url = createUrl(config),
        driver = "com.amazon.redshift.jdbc42.Driver",
        user = config.dbUser,
        password = config.dbPassword,
        prop = new Properties(),
        executor = executor
      )

      Future(Right(new RedshiftConnector(db, connectorConfig)))

    } else {
      Future(Left(ErrorWithMessage("SSL Error")))
    }
  }

  override def meta() = MetaData("\"", "'", "\\")

  private[redshift] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*ssl=false.*")
  }

  private[redshift] def createUrl(config: RedshiftConnectionConfig) = {
    s"jdbc:redshift://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private[redshift] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}