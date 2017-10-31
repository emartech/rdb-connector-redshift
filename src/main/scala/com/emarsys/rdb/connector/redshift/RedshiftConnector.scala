package com.emarsys.rdb.connector.redshift

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.{ExecutionContext, Future}

class RedshiftConnector (db: Database)(implicit executionContext: ExecutionContext) extends Connector {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int]).map(_ => Right()).recover{ case _ => Left(ErrorWithMessage("Cannot connect to the sql server")) }
  }

}

object RedshiftConnector {
  case class RedshiftConnectionConfig(
                                       host: String,
                                       port: Int,
                                       dbName: String,
                                       dbUser: String,
                                       dbPassword: String,
                                       connectionParams: String
                                  )


  def apply(config: RedshiftConnectionConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[RedshiftConnector] = {

    val db = Database.forURL(
      url = createUrl(config),
      driver = "com.amazon.redshift.jdbc42.Driver",
      user = config.dbUser,
      password = config.dbPassword,
      prop = new Properties(),
      executor = executor
    )

    Future(Right(new RedshiftConnector(db)))
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