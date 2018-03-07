package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, FieldValueWrapper, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import slick.jdbc.PostgresProfile.api._
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.DefaultSqlWriters._
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.NullValue

import scala.concurrent.Future

trait RedshiftRawDataManipulation {

  self: RedshiftConnector =>

  override def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    if(definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql
      val queries = definitions.map { definition =>
        val setPart = createSetQueryPart(definition.update)
        val wherePart = createConditionQueryPart(definition.search).toSql
        sqlu"UPDATE #$table SET #$setPart WHERE #$wherePart"
      }

      db.run(DBIO.sequence(queries).transactionally)
        .map(results => Right(results.sum))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    if(definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val query = createInsertQuery(tableName, definitions)

      db.run(query)
        .map(result => Right(result))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawUpsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    if (definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      getPrimaryKeyFields(tableName).flatMap((primaryKeyFields: Seq[String]) => {
        if (primaryKeyFields.isEmpty) {
          rawInsertData(tableName, definitions)
        } else {

          val primaryKeyDefinitions = filterPrivateKeyDefinitions(primaryKeyFields, definitions)
          val query = (for {
            _           <- createDeleteQuery(tableName, primaryKeyDefinitions)
            insertCount <- createInsertQuery(tableName, definitions)
          } yield insertCount).transactionally

          db.run(query).map(Right(_))
        }
      }).recover {
        case ex =>
          Left(ErrorWithMessage(ex.toString))
      }
    }
  }

  override def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    if (criteria.isEmpty) {
      Future.successful(Right(0))
    } else {
      val query = createDeleteQuery(tableName, criteria)

      db.run(query)
        .map(result => Right(result))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    val newTableName = generateTempTableName(tableName)
    val newTable = TableName(newTableName).toSql
    val table = TableName(tableName).toSql
    val createTableQuery = sqlu"CREATE TABLE #$newTable ( LIKE #$table INCLUDING DEFAULTS )"
    val dropTableQuery = sqlu"DROP TABLE IF EXISTS #$newTable"

    db.run(createTableQuery)
      .flatMap( _ =>
        rawInsertData(newTableName, definitions).flatMap( insertedCount =>
          swapTableNames(tableName, newTableName).flatMap( _ =>
            db.run(dropTableQuery).map( _ => insertedCount )
          )
        )
      )
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  private def swapTableNames(tableName: String, newTableName: String): Future[Seq[Int]] = {
    val temporaryTableName = generateTempTableName()
    val tablePairs =  Seq((tableName, temporaryTableName), (newTableName, tableName), (temporaryTableName, newTableName))
    val queries = tablePairs.map({
      case (from, to) => TableName(from).toSql + " TO " + TableName(to).toSql
        sqlu"ALTER TABLE #${TableName(from).toSql} RENAME TO #${TableName(to).toSql}"
    })
    db.run(DBIO.sequence(queries).transactionally)
  }

  private def generateTempTableName(original: String = ""): String = {
    val shortedName = if(original.length > 30) original.take(30) else original
    val id = java.util.UUID.randomUUID().toString.replace("-","").take(30)
    shortedName + "_" + id
  }

  private def orderValues(data: Seq[Record], orderReference: Seq[String]): Seq[Seq[FieldValueWrapper]] = {
    data.map(row => orderReference.map(d => row.getOrElse(d, NullValue)))
  }

  private def makeSqlValueList(data: Seq[Seq[FieldValueWrapper]]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    data.map(_.map(_.toSimpleSelectValue.map(_.toSql).getOrElse("NULL")).mkString(", "))
      .mkString("(", "),(", ")")
  }

  private def createConditionQueryPart(criteria: Criteria) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    And(criteria.mapValues(_.toSimpleSelectValue).map {
      case (field, Some(value)) => EqualToValue(FieldName(field), value)
      case (field, None)        => IsNull(FieldName(field))
    }.toList)
  }

  private def createSetQueryPart(criteria: Map[String, FieldValueWrapper]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    criteria.mapValues(_.toSimpleSelectValue).map {
      case (field, Some(value)) => EqualToValue(FieldName(field), value).toSql
      case (field, None)        => FieldName(field).toSql + "=NULL"
    }.mkString(", ")
  }

  private def filterPrivateKeyDefinitions(primaryKeyFields: Seq[String], definitions: Seq[Record]): Seq[Record] = {
    convertToLowerCaseFieldNames(definitions)
      .map(_.filterKeys(primaryKeyFields.contains))
  }

  private def convertToLowerCaseFieldNames(definitions: Seq[Record]): Seq[Record] = {
    definitions.map(_.map { case (k, v) => (k.toLowerCase, v) })
  }

  private def createInsertQuery(tableName: String, definitions: Seq[Record]) = {
    val table = TableName(tableName).toSql

    val fields = definitions.head.keySet.toSeq
    val fieldList = fields.map(FieldName(_).toSql).mkString("(", ",", ")")
    val valueList = makeSqlValueList(orderValues(definitions, fields))

    sqlu"INSERT INTO #$table #$fieldList VALUES #$valueList"
  }

  private def createDeleteQuery(tableName: String, criteria: Seq[Criteria]) = {
    val table = TableName(tableName).toSql
    val condition = Or(criteria.map(createConditionQueryPart)).toSql
    sqlu"DELETE FROM #$table WHERE #$condition"
  }

  private def getPrimaryKeyFields(tableName: String): Future[Seq[String]] = {
    val table = Value(tableName).toSql
    db.run(sql"""SELECT
                     f.attname
                 FROM pg_attribute f
                     JOIN pg_class c ON c.oid = f.attrelid
                     JOIN pg_type t ON t.oid = f.atttypid
                     LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
                     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                     LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
                     LEFT JOIN pg_class AS g ON p.confrelid = g.oid
                 WHERE c.relkind = 'r'::char
                     AND c.relname = #$table
                     AND p.contype = 'p'
                     AND f.attnum > 0""".as[String])
  }

}
