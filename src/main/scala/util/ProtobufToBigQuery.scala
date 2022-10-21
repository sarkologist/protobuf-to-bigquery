package util

import util.Protobuf._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.protobuf.Descriptors.FieldDescriptor.Type._
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Message}
import scalaz.Yoneda

import java.util
import scala.jdk.CollectionConverters._

object ProtobufToBigQuery extends Serializable {

  val _set = "_set"

  def toTableSchemaType(t: FieldDescriptor.Type): String = t match {
    case DOUBLE | FLOAT => "FLOAT"
    case INT32 | UINT32 | SINT32 | FIXED32 | SFIXED32 | INT64 | UINT64 |
        SINT64 | FIXED64 | SFIXED64 =>
      "INTEGER"
    case BOOL            => "BOOLEAN"
    case STRING          => "STRING"
    case BYTES           => "BYTES"
    case ENUM            => "STRING"
    case MESSAGE | GROUP => "RECORD"
  }

  /** protobuf messages have potentially unlimited depth, since
   * message-inside-message recursion is possible.
   * that doesn't mean we cannot produce a schema though
   * provided at runtime messages written to the table are never
   * beyond the max depth
   * see: https://cloud.google.com/bigquery/docs/nested-repeated#limitations
   */
  def makeTableSchema(descriptor: Descriptor): TableSchema = {
    def base(
        f: FieldDescriptor): Either[TableFieldSchema, List[TableFieldSchema]] =
      Left(makeField(f))

    def recurse(
        children: List[(Either[TableFieldSchema, List[TableFieldSchema]],
                        FieldDescriptor)])
      : Either[TableFieldSchema, List[TableFieldSchema]] = Right {
      children
        .foldLeft(List.empty[TableFieldSchema]) {
          case (fields, (Left(zero), _)) => zero :: fields
          case (fields, (Right(many), f)) =>
            val fs =
              if (!f.isRepeated && hitPotentialBottom(f.getMessageType) && needsToFlagPresence(
                    f))
                many ::: List(
                  new TableFieldSchema()
                    .setName(_set)
                    .setType("BOOLEAN")
                    .setMode("NULLABLE"))
              else many
            makeField(f).setFields(fs.asJava) :: fields
        }
    }

    foldDescriptor(base, recurse, 15)(descriptor)
      .fold(_ => throw new RuntimeException,
            fields => new TableSchema().setFields(fields.asJava))
  }

  def makeField(f: FieldDescriptor): TableFieldSchema =
    new TableFieldSchema()
      .setName(bigqueryColumnName(f))
      .setType(toTableSchemaType(f.getType))
      .setMode(if (f.isRepeated) "REPEATED" else "NULLABLE")

  def bigqueryColumnName(f: FieldDescriptor): String = {
    f.getName.replace(".", "_")
  }

  /** unlike `hitBottom` this is static,
   *  i.e. a property of the `Descriptor` schema, not of the `Message` value
   *
   * the bottom "potential" in the sense that a value of `Message` may bottom-out for this `Descriptor`,
   * even though *schema-wise* a `Message` can have potentially infinite-depth
   */
  def hitPotentialBottom(d: Descriptor): Boolean = {
    d.getFields.asScala.forall(f =>
      f.getType match {
        case MESSAGE | GROUP => !f.isRequired
        case _               => true
    })
  }

  /**
   * note that this is not static, i.e.
   * i.e. a property of the `Message` value at runtime not just the `Descriptor` schema
   *
   * *schema-wise* a `Message` can have potentially infinite-depth
   */
  def hitActualBottom(message: Message): Boolean = {
    def doesNotRecurse(fieldDescriptor: FieldDescriptor): Boolean =
      fieldDescriptor.getType match {
        case MESSAGE | GROUP => false
        case _               => true
      }
    message.getAllFields.asScala.keys.forall(doesNotRecurse)
  }

  def hasAlwaysPresentFields(descriptor: Descriptor): Boolean =
    descriptor.getFields.asScala
      .exists(f => isAlwaysPresentField(oneOfFields(descriptor), f))

  def isAlwaysPresentField(oneOfFields: Set[FieldDescriptor],
                           f: FieldDescriptor): Boolean =
    f.getType match {
      // only proto2 have required fields
      // but for proto3 .isRequired is always false
      // oneof fields don't have default values
      // optional primitive-type fields have default values
      case MESSAGE | GROUP => false
      case _ =>
        !oneOfFields.contains(f) && (f.isOptional || f.isRequired)
    }

  def makeTableRow(msg: Message,
                   customRow: (FieldDescriptor,
                     Yoneda[Repeated, Any]) => Yoneda[Repeated, Any]
                   = { case (_, x) => x })
    : TableRow =
    {
      def base(value: AnyRef, f: FieldDescriptor): Any =
        toBigQueryPrimitiveType(f.getType)(value)

      def recurse(
          children: Seq[(Yoneda[Repeated, (Any, Message)], FieldDescriptor)])
        : Any =
        children.foldLeft(new TableRow) {
          case (tr, (repeated, f)) =>
            val value = customRow(
              f,
              repeated
                .map {
                  case (child: TableRow, msg) =>
                    if (!f.isRepeated && hitActualBottom(msg) && needsToFlagPresence(
                          f))
                      child.set(_set, true)
                    else child
                  case (primitive, _) => primitive
                }
            ).run.underlying

            tr.set(bigqueryColumnName(f), value)
        }

      foldMessage(recurse, base)(msg).asInstanceOf[TableRow]
    }

  // useless in practice, only useful for testing
  def fillInDefaultValues[A <: Message](message: A): A = {
    val builder = message.newBuilderForType

    val recursiveCase = message.getAllFields.asScala.map {
      case (f, v) =>
        val newV =
          if (!f.isRepeated)
            f.getType match {
              case MESSAGE | GROUP =>
                fillInDefaultValues(v.asInstanceOf[Message])
              case _ => v
            } else
            f.getType match {
              case MESSAGE | GROUP =>
                v.asInstanceOf[util.List[Message]]
                  .asScala
                  .map(fillInDefaultValues)
                  .asJava
              case _ => v
            }

        f -> newV
    }.toSet

    val baseCase = defaultValues(message)()

    (recursiveCase ++ baseCase)
      .foreach {
        case (f, v) =>
          builder.setField(f, v)
      }

    builder.build.asInstanceOf[A]
  }

  def needsToFlagPresence(f: FieldDescriptor): Boolean =
    !(f.isRequired || hasAlwaysPresentFields(f.getMessageType))

  def toBigQueryPrimitiveType(f: FieldDescriptor.Type)(v: AnyRef): Any = f match {
    case DOUBLE => v.asInstanceOf[Double]
    case FLOAT  => v.asInstanceOf[Float]
    case INT32 | UINT32 | SINT32 | FIXED32 | SFIXED32 =>
      v.asInstanceOf[Int]
    case INT64 | UINT64 | SINT64 | FIXED64 | SFIXED64 =>
      v.asInstanceOf[Long]
    case BOOL   => v.asInstanceOf[Boolean]
    case STRING => v.asInstanceOf[String]
    //jackson json convertor usec by BigQueryIO.Write
    //cannot recognize ByteString object type
    //need to use ByteArray instead
    case BYTES => v.asInstanceOf[ByteString].toByteArray
    case ENUM  => v.asInstanceOf[EnumValueDescriptor].getName
  }

}
