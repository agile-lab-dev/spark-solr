package it.agilelab.bigdata.solr

import java.sql.Timestamp
import java.util.Date

import com.lucidworks.spark.util.SolrQuerySupport
import com.lucidworks.spark.SolrSupport
import com.lucidworks.spark.SolrRDD
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SolrDataframe {
  var log: Logger = Logger.getLogger(classOf[SolrDataframe])

  def getBaseSchema(
                     zkHost: String,
                     collection: String,
                     escapeFields: Boolean,
                     flattenMultivalued: Boolean): StructType =
    getBaseSchema(Set.empty[String], zkHost, collection, escapeFields, flattenMultivalued)

  def getBaseSchema(
                     fields: Set[String],
                     zkHost: String,
                     collection: String,
                     escapeFields: Boolean,
                     flattenMultivalued: Boolean): StructType = {
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val fieldTypeMap = SolrQuerySupport.getFieldTypes(fields, solrBaseUrl, collection)
    val structFields = new ListBuffer[StructField]

    fieldTypeMap.foreach{ case(fieldName, fieldMeta) =>
      val metadata = new MetadataBuilder
      var dataType: DataType = {
        if (fieldMeta.fieldTypeClass.isDefined) {
          if (SolrQuerySupport.SOLR_DATA_TYPES.contains(fieldMeta.fieldTypeClass.get)) {
            SolrQuerySupport.SOLR_DATA_TYPES(fieldMeta.fieldTypeClass.get)
          } else {
            DataTypes.StringType
          }
        }
        else
          DataTypes.StringType
      }

      metadata.putString("name", fieldName)
      metadata.putString("type", fieldMeta.fieldType)

      if (!flattenMultivalued && fieldMeta.isMultiValued.isDefined) {
        if (fieldMeta.isMultiValued.get) {
          dataType = new ArrayType(dataType, true)
          metadata.putBoolean("multiValued", value = true)
        }
      }

      if (fieldMeta.isRequired.isDefined)
        metadata.putBoolean("required", value = fieldMeta.isRequired.get)

      if (fieldMeta.isDocValues.isDefined)
        metadata.putBoolean("docValues", value = fieldMeta.isDocValues.get)

      if (fieldMeta.isStored.isDefined)
        metadata.putBoolean("stored", value = fieldMeta.isStored.get)

      if (fieldMeta.fieldTypeClass.isDefined)
        metadata.putString("class", fieldMeta.fieldTypeClass.get)

      if (fieldMeta.dynamicBase.isDefined)
        metadata.putString("dynamicBase", fieldMeta.dynamicBase.get)

      val name = if (escapeFields) fieldName.replaceAll("\\.", "_") else fieldName

      structFields.add(DataTypes.createStructField(name, dataType, !fieldMeta.isRequired.getOrElse(false), metadata.build()))
    }

    DataTypes.createStructType(structFields.toList)
  }

  def docToRows(schema: StructType, docs: RDD[SolrDocument]): RDD[Row] = {
    val fields = schema.fields

    val rows = docs.map(solrDocument => {
      val values = new ListBuffer[AnyRef]
      for (field <- fields) {
        val metadata = field.metadata
        val isMultiValued = if (metadata.contains("multiValued")) metadata.getBoolean("multiValued") else false
        if (isMultiValued) {
          val fieldValues = solrDocument.getFieldValues(field.name)
          if (fieldValues != null) {
            val iterableValues = fieldValues.iterator().map {
              case d: Date => new Timestamp(d.getTime)
              case i: java.lang.Integer => new java.lang.Long(i.longValue())
              case f: java.lang.Float => new java.lang.Double(f.doubleValue())
              case a => a
            }
            values.add(iterableValues.toArray)
          } else {
            values.add(null)
          }

        } else {
          val fieldValue = solrDocument.getFieldValue(field.name)
          fieldValue match {
            case f: String => values.add(f)
            case f: Date => values.add(new Timestamp(f.getTime))
            case i: java.lang.Integer => values.add(new java.lang.Long(i.longValue()))
            case f: java.lang.Float => values.add(new java.lang.Double(f.doubleValue()))
            case f: java.util.ArrayList[_] =>
              val jlist = f.iterator.map {
                case d: Date => new Timestamp(d.getTime)
                case i: java.lang.Integer => new java.lang.Long(i.longValue())
                case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                case v: Any => v
              }
              val arr = jlist.toArray
              if (arr.length >= 1) {
                values.add(arr(0).asInstanceOf[AnyRef])
              }
            case f: Iterable[_] =>
              val iterableValues = f.iterator.map {
                case d: Date => new Timestamp(d.getTime)
                case i: java.lang.Integer => new java.lang.Long(i.longValue())
                case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                case v: Any => v
              }
              val arr = iterableValues.toArray
              if (!arr.isEmpty) {
                values.add(arr(0).asInstanceOf[AnyRef])
              }
            case f: Any => values.add(f)
            case f => values.add(f)
          }
        }
      }
      Row(values.toArray:_*)
    })
    rows
  }
}

class SolrDataframe(sparkSession: SparkSession, zkHosts: String, collection: String, query: Option[String]) {

  val df: DataFrame = retrieveDataFrame()

  private def retrieveDataFrame(): DataFrame = {

    //Get Base schema for the collection
    //TODO review boolean params
    val dfStruct: StructType = SolrDataframe.getBaseSchema(zkHosts, collection, false, false)

    val getAllQuery = new SolrQuery(query.getOrElse("*:*"))
    val solrRdd = new SolrRDD(zkHosts, collection)
    val rddSolrDoc = solrRdd
      .query(sparkSession.sparkContext, getAllQuery, false /* TODO deepPaging better for performances*/)
      .rdd

   val rddRows = SolrDataframe.docToRows(dfStruct, rddSolrDoc)

    sparkSession.createDataFrame(rddRows, dfStruct)
  }


}
