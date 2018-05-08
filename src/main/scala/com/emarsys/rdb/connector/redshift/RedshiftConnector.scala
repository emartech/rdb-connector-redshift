package com.emarsys.rdb.connector.redshift

import java.util.{Properties, UUID}

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ConnectionConfigError
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.redshift.RedshiftConnector.{RedshiftConnectionConfig, RedshiftConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class RedshiftConnector(
                         protected val db: Database,
                         protected val connectorConfig: RedshiftConnectorConfig,
                         protected val poolName: String,
                         protected val schemaName: String
                       )(
                         implicit val executionContext: ExecutionContext
                       )
  extends Connector
    with RedshiftTestConnection
    with RedshiftErrorHandling
    with RedshiftMetadata
    with RedshiftSimpleSelect
    with RedshiftRawSelect
    with RedshiftIsOptimized
    with RedshiftRawDataManipulation {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory
    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
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
                                     ) extends ConnectionConfig {
    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("redshift", s"$host:$port", dbName, dbUser)
    }
  }

  case class RedshiftConnectorConfig(
                                      queryTimeout: FiniteDuration,
                                      streamChunkSize: Int
                                    )

}

trait RedshiftConnectorTrait extends ConnectorCompanion {
  private[redshift] val defaultConfig = RedshiftConnectorConfig(
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

      val poolName = UUID.randomUUID.toString

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
            .withValue("redshiftdb.poolName", ConfigValueFactory.fromAnyRef(poolName))
            .withValue("redshiftdb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
            .withValue("redshiftdb.properties.url", ConfigValueFactory.fromAnyRef(createUrl(config)))
            .withValue("redshiftdb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
            .withValue("redshiftdb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
            .withValue("redshiftdb.properties.driver", ConfigValueFactory.fromAnyRef("com.amazon.redshift.jdbc42.Driver"))

          Database.forConfig("redshiftdb", customDbConf)
        }

      Future(Right(new RedshiftConnector(db, connectorConfig, poolName, createSchemaName(config))))

    } else {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    }
  }

  override def meta() = MetaData("\"", "'", "\\")

  private[redshift] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*ssl=false.*")
  }

  private[redshift] def createUrl(config: RedshiftConnectionConfig) = {
    s"jdbc:redshift://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def createSchemaName(config: RedshiftConnectionConfig) = {
    config.connectionParams
      .split("&").toList
      .find(_.startsWith("currentSchema="))
      .flatMap(_.split("=").toList.tail.headOption)
      .getOrElse("public")
  }
  private[redshift] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}