package com.emarsys.rdb.connector.redshift

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector, ConnectorCompanion, MetaData}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.redshift.RedshiftConnector.{RedshiftConnectionConfig, RedshiftConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
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

object RedshiftConnector extends RedshiftConnectorTrait {

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

}

trait RedshiftConnectorTrait extends ConnectorCompanion {
  private val defaultConfig = RedshiftConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  val useHikari: Boolean = Config.db.useHikari

  def apply(
             config: RedshiftConnectionConfig,
             connectorConfig: RedshiftConnectorConfig = defaultConfig
           )(
             executor: AsyncExecutor
           )(
             implicit executionContext: ExecutionContext
           ): ConnectorResponse[RedshiftConnector] = {

    if (checkSsl(config.connectionParams)) {

      val db =
        if(!useHikari) {
          Database.forURL(
            url = createUrl(config),
            driver = "com.amazon.redshift.jdbc42.Driver",
            user = config.dbUser,
            password = config.dbPassword,
            prop = new Properties(),
            executor = executor
          )
        } else {
          val customDbConf = ConfigFactory.load()
            .withValue("redshiftdb.properties.url", ConfigValueFactory.fromAnyRef(createUrl(config)))
            .withValue("redshiftdb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
            .withValue("redshiftdb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
            .withValue("redshiftdb.properties.driver", ConfigValueFactory.fromAnyRef("com.amazon.redshift.jdbc42.Driver"))

          Database.forConfig("redshiftdb", customDbConf)
        }

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