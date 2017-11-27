package com.lucidworks.spark.util

import java.util

import com.lucidworks.spark.SolrSupport
import org.apache.solr.common.{SolrDocument, SolrException}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import com.lucidworks.spark.util.JsonUtil._
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class SolrFieldMeta(
                          fieldType: String,
                          dynamicBase: Option[String],
                          isRequired: Option[Boolean],
                          isMultiValued: Option[Boolean],
                          isDocValues: Option[Boolean],
                          isStored: Option[Boolean],
                          fieldTypeClass: Option[String])

object SolrQuerySupport {

  val SOLR_DATA_TYPES: Map[String, DataType] = HashMap(
    "solr.StrField" -> DataTypes.StringType,
    "solr.TextField" -> DataTypes.StringType,
    "solr.BoolField" -> DataTypes.BooleanType,
    "solr.TrieIntField" -> DataTypes.LongType,
    "solr.TrieLongField" -> DataTypes.LongType,
    "solr.TrieFloatField" -> DataTypes.DoubleType,
    "solr.TrieDoubleField" -> DataTypes.DoubleType,
    "solr.TrieDateField" -> DataTypes.TimestampType,
    "solr.BinaryField" -> DataTypes.BinaryType
  )

  def getUniqueKey(zkHost: String, collection: String): String = {
    try {
      val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
      // Hit Solr Schema API to get base information
      val schemaUrl: String = solrBaseUrl + collection + "/schema"
      try {
        val schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), schemaUrl, 2)
        if (schemaMeta.has("schema") && (schemaMeta \ "schema").has("uniqueKey")) {
          schemaMeta \ "schema" \ "uniqueKey" match {
            case v: JString => return v.s
            case v: Any => throw new Exception("Unexpected type '" + v.getClass + "' other than JString for uniqueKey '" + v + "'");
          }
        }
      }
      catch {
        case solrExc: SolrException =>
          //TODO handle exceptions
//          log.warn("Can't get uniqueKey for " + collection + " due to solr: " + solrExc)
        throw solrExc
      }
    } catch {
      case e: Exception => throw e
    }
    QueryConstants.DEFAULT_REQUIRED_FIELD
  }

  def getFieldTypes(fields: Set[String], solrBaseUrl: String, collection: String): Map[String, SolrFieldMeta] =
    getFieldTypes(fields, solrBaseUrl + collection + "/")

  def getFieldTypes(fields: Set[String], solrUrl: String): Map[String, SolrFieldMeta] = {
    val fieldTypeMap = new mutable.HashMap[String, SolrFieldMeta]()
    val fieldTypeToClassMap = getFieldTypeToClassMap(solrUrl)
    val fieldNames = if (fields == null || fields.isEmpty) getFieldsFromLuke(solrUrl) else fields
    val fieldDefinitionsFromSchema = getFieldDefinitionsFromSchema(solrUrl, fieldNames)
    fieldDefinitionsFromSchema.foreach{ case(name, payloadRef) =>
      payloadRef match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
          val payload = m.asInstanceOf[Map[String, Any]]
          // No valid checks for name and value :(
          val name = payload.get("name").get.asInstanceOf[String]
          val fieldType = payload.get("type").get.asInstanceOf[String]

          val isRequired: Option[Boolean] = {
            if (payload.contains("required")) {
              if (payload.get("required").isDefined) {
                payload.get("required").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isMultiValued: Option[Boolean] = {
            if (payload.contains("multiValued")) {
              if (payload.get("multiValued").isDefined) {
                payload.get("multiValued").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isStored: Option[Boolean] = {
            if (payload.contains("stored")) {
              if (payload.get("stored").isDefined) {
                payload.get("stored").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val isDocValues: Option[Boolean] = {
            if (payload.contains("docValues")) {
              if (payload.get("docValues").isDefined) {
                payload.get("docValues").get match {
                  case v: Boolean => Some(v)
                  case v1: AnyRef => Some(String.valueOf(v1).equals("true"))
                }
              } else None
            } else None
          }

          val dynamicBase: Option[String] = {
            if (payload.contains("dynamicBase")) {
              if (payload.get("dynamicBase").isDefined) {
                payload.get("dynamicBase").get match {
                  case v: String => Some(v)
                }
              } else None
            } else None
          }

          val fieldClassType: Option[String] = {
            if (fieldTypeToClassMap.contains(fieldType)) {
              if (fieldTypeToClassMap.get(fieldType).isDefined) {
                Some(fieldTypeToClassMap.get(fieldType).get)
              } else None
            } else None
          }

          val solrFieldMeta = SolrFieldMeta(fieldType, dynamicBase, isRequired, isMultiValued, isDocValues, isStored, fieldClassType)

          /*if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
            (solrFieldMeta.isDocValues.isDefined && !solrFieldMeta.isDocValues.get)) {
            if (log.isDebugEnabled)
              log.debug("Can't retrieve an index only field: '" + name + "'. Field info " + payload)
          } else */
          /*if ((solrFieldMeta.isStored.isDefined && !solrFieldMeta.isStored.get) &&
            (solrFieldMeta.isMultiValued.isDefined && solrFieldMeta.isMultiValued.get) &&
            (solrFieldMeta.isDocValues.isDefined && solrFieldMeta.isDocValues.get)) {
            if (log.isDebugEnabled)
              log.debug("Can't retrieve a non-stored multiValued docValues field: '" + name + "'. The payload info is " + payload)
          } else {*/
            fieldTypeMap.put(name, solrFieldMeta)
//          }
        case somethingElse: Any => //log.warn("Unknown class type '" + somethingElse.getClass.toString + "'")
      }
    }

//    if (fieldTypeMap.isEmpty)
//      log.warn("No readable fields found!")
    fieldTypeMap.toMap
  }

  def getFieldTypeToClassMap(solrUrl: String) : Map[String, String] = {
    val fieldTypeToClassMap: mutable.Map[String, String] = new mutable.HashMap[String, String]
    val fieldTypeUrl = solrUrl + "schema/fieldtypes"
    try {
      val fieldTypeMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldTypeUrl, 2)
      if (fieldTypeMeta.has("fieldTypes")) {
        (fieldTypeMeta \ "fieldTypes").values match {
          case types: List[Any] =>
            if (types.nonEmpty) {
              // Get the name, type and add them to the map
              types.foreach {
                case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
                  val fieldTypePayload = m.asInstanceOf[Map[String, Any]]
                  if (fieldTypePayload.contains("name") && fieldTypePayload.contains("class")) {
                    val fieldTypeName = fieldTypePayload.get("name")
                    val fieldTypeClass = fieldTypePayload.get("class")
                    if (fieldTypeName.isDefined && fieldTypeClass.isDefined) {
                      fieldTypeName.get match {
                        case name: String =>
                          fieldTypeClass.get match {
                            case typeClass: String =>
                              fieldTypeToClassMap.put(name, typeClass)
                          }
                      }
                    }
                  }
              }
            }
          case t: AnyRef => //TODO handle logging log.warn("Found unexpected object type '" + t + "' when parsing field types json")
        }
      }
    } catch {
      case e: Exception =>
        //TODO handle logging log.error("Can't get field type metadata from Solr url " + fieldTypeUrl)
        e match {
          case e1: RuntimeException => throw e1
          case e2: Exception => throw new RuntimeException(e2)
        }
    }

    fieldTypeToClassMap.toMap
  }


  def getFieldsFromLuke(solrUrl: String): Set[String] = {
    val lukeUrl: String = solrUrl + "admin/luke?numTerms=0"
    try {
      val adminMeta: JValue = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), lukeUrl, 2)
      if (!adminMeta.has("fields")) {
        throw new Exception("Cannot find 'fields' payload inside Schema: " + compact(adminMeta))
      }
      val fieldsRef = adminMeta \ "fields"
      fieldsRef.values match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String]) => m.asInstanceOf[Map[String, Any]].keySet
        case somethingElse: Any =>  throw new Exception("Unknown type '" + somethingElse.getClass + "'")
      }
    } catch {
      case e1: Exception =>
        //TODO handle logging log.warn("Can't get schema fields from url " + lukeUrl + " due to: " + e1)
        throw e1
    }
  }

  def getFieldDefinitionsFromSchema(solrUrl: String, fieldNames: Set[String]): Map[String, Any] = {
    val fl: Option[String] = if (fieldNames.nonEmpty) {
      val sb = new StringBuilder
      sb.append("&fl=")
      fieldNames.zipWithIndex.foreach{ case(name, index) =>
        sb.append(name)
        if (index < fieldNames.size) sb.append(",")
      }
      Some(sb.toString())
    } else None

    val fieldsUrl = solrUrl + "schema/fields?showDefaults=true&includeDynamic=true" + fl.getOrElse("")

    try {
      SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient, fieldsUrl, 2).values match {
        case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
          val payload = m.asInstanceOf[Map[String, Any]]
          if (payload.contains("fields")) {
            if (payload.get("fields").isDefined) {
              payload.get("fields").get match {
                case fields: List[Any] =>
                  constructFieldInfoMap(fields)
              }
            } else {
              throw new Exception("No fields payload inside the response: " + payload)
            }
          } else {
            throw new Exception("No fields payload inside the response: " + payload)
          }
        case somethingElse: Any => throw new Exception("Unknown type '" + somethingElse.getClass + "' from schema object " + somethingElse)
      }

    } catch {
      case e: Exception =>
        //TODO handle logging log.error("Can't get field metadata from Solr using request '" + fieldsUrl + "' due to exception " + e)
        e match {
          case e1: RuntimeException => throw e1
          case e2: Exception => throw new RuntimeException(e2)
        }
    }
  }

  def constructFieldInfoMap(fieldsInfoList: List[Any]): Map[String, Any] = {
    val fieldInfoMap = new mutable.HashMap[String, AnyRef]()
    fieldsInfoList.foreach {
      case m: Map[_, _] if m.keySet.forall(_.isInstanceOf[String])=>
        val fieldInfo = m.asInstanceOf[Map[String, Any]]
        if (fieldInfo.contains("name")) {
          val fieldName = fieldInfo.get("name")
          if (fieldName.isDefined) {
            fieldInfoMap.put(fieldName.get.asInstanceOf[String], fieldInfo)
          } else {
//            log.info("value for key 'name' is not defined in the payload " + fieldInfo)
          }
        } else {
//          log.info("'name' is not defined in the payload " + fieldInfo)
        }
      case somethingElse: Any => throw new Exception("Unknown type '" + somethingElse.getClass)
    }
    fieldInfoMap.toMap
  }

}
