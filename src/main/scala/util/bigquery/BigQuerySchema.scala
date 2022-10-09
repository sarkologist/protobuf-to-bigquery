package util.bigquery

import com.google.api.client.util.GenericData
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema}
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.Type.{GROUP, MESSAGE}
import com.google.protobuf.Message

import java.util
import scala.jdk.CollectionConverters._

object BigQuerySchema {

  trait IsBigQuerySchema[S] {
    def toSchema(s: S): Schema

    def toTableSchema(s: S): TableSchema
  }

  implicit val schemaIsTableSchema: IsBigQuerySchema[Schema] =
    new IsBigQuerySchema[Schema] {
      override def toSchema(schema: Schema): Schema = schema

      override def toTableSchema(schema: Schema): TableSchema =
        schemaToTableSchema(schema)
    }

  implicit val tableSchemaIsTableSchema: IsBigQuerySchema[TableSchema] =
    new IsBigQuerySchema[TableSchema] {
      override def toSchema(tableSchema: TableSchema): Schema =
        tableSchemaToSchema(tableSchema)

      override def toTableSchema(tableSchema: TableSchema): TableSchema =
        tableSchema
    }

  def tableSchemaToSchema(schema: TableSchema): Schema =
    Schema.of(schema.getFields.asScala.map(toField).asJava)

  def toField(field: TableFieldSchema): Field =
    Field
      .newBuilder(
        field.getName,
        LegacySQLTypeName.valueOf(canonicalTypeNameFor(field.getType)),
        Option(field.getFields)
          .map(_.asScala.toSeq)
          .getOrElse(List())
          .map(toField): _*
      )
      .setMode(Mode.valueOf(field.getMode))
      .setDescription(field.getDescription)
      .build()

  def schemaToTableSchema(schema: Schema): TableSchema =
    new TableSchema().setFields(
      (0 until schema.getFields
        .size()).map(i => fromField(schema.getFields.get(i))).asJava
    )

  def fromField(field: Field): TableFieldSchema =
    new TableFieldSchema()
      .setName(field.getName)
      .setMode(Option(field.getMode).map(_.name).getOrElse("NULLABLE"))
      .setType(field.getType.getStandardType.name)
      .setDescription(field.getDescription)
      .setFields(
        (0 until Option(field.getSubFields).map(_.size).getOrElse(0))
          .map(i => fromField(field.getSubFields.get(i)))
          .asJava
      )

  def canonicalTypeNameFor(typ: String): String =
    Map(
      "INT64"   -> "INTEGER",
      "FLOAT64" -> "FLOAT",
      "BOOL"    -> "BOOLEAN",
      "STRUCT"  -> "RECORD"
    ).getOrElse(typ, typ)

  sealed trait Diff {
    val context: Seq[String]

    def displayWithContext(s: String): String = s"${context.mkString("/")}: $s"
  }

  case class DiffType(context: Seq[String], left: String, right: String)
      extends Diff

  case class DiffMode(context: Seq[String], left: String, right: String)
      extends Diff

  case class DiffDescription(context: Seq[String], left: String, right: String)
      extends Diff

  case class DiffFields(
      context: Seq[String],
      leftOnly: Set[String],
      rightOnly: Set[String]
  ) extends Diff

  def displayDiff(diff: Diff): String = diff match {
    case DiffMode(_, l, r) =>
      diff.displayWithContext(s"different modes! $l vs. $r")
    case DiffDescription(_, l, r) =>
      diff.displayWithContext(s"different descriptions! $l vs. $r")
    case DiffType(_, l, r) =>
      diff.displayWithContext(s"different types! $l vs. $r")
    case DiffFields(_, leftOnly, rightOnly) =>
      diff.displayWithContext(
        s"l\\r: ${leftOnly.mkString(",")}" + s", r\\l: ${rightOnly.mkString(",")}"
      )
  }

  def cannotMigrate(diff: Diff): Boolean = diff match {
    case DiffMode(_, "REQUIRED", "NULLABLE") => false
    case DiffMode(_, _, _)                   => true
    case DiffDescription(_, _, _)            => false
    case DiffType(_, _, _)                   => true
    case DiffFields(_, o, n)                 => !o.forall(n.contains)
  }

  def willCauseFailedWrite(diff: Diff): Boolean = diff match {
    case DiffMode(_, "REQUIRED", "NULLABLE") => false
    case DiffMode(_, _, _)                   => true
    case DiffType(_, _, _)                   => true
    case DiffDescription(_, _, _)            => false
    case DiffFields(_, o, n)                 => !n.forall(o.contains)
  }

  // returns list of "problematic" diffs
  // what is "problematic" depends on `check`
  // https://cloud.google.com/bigquery/docs/managing-table-schemas
  def checkSchema[S: IsBigQuerySchema, T: IsBigQuerySchema](
      check: Diff => Boolean
  )(old: S, neu: T): Option[Iterable[Diff]] = {
    optionNonEmpty(compareSchema(old, neu).filter(check))
  }

  sealed trait Segment

  case class Key(key: String) extends Segment

  case class Index(index: Int) extends Segment

  def isEmptyGenericData(o: Object): Boolean =
    o match {
      case genericData: GenericData => genericData.isEmpty
      case _                        => false
    }

  // WARNING: this mutates the GenericData
  // couldn't get it to clone
  def normaliseEmptyGenericData(o: Object): Object =
    o match {
      case genericData: GenericData =>
        genericData.entrySet.asScala.map(e => e.getKey -> e.getValue).foreach {
          case (k, v) => if (isEmptyGenericData(v)) genericData.remove(k)
        }
        genericData
      case _ => o
    }

  def pathsOf(message: Message): Seq[Seq[Segment]] = pathsOf(Seq.empty, message)

  def walk(path: Seq[Segment], message: Message): Option[Object] =
    walkMessage(path, message.asInstanceOf[Object])

  // although getters/setters and Message.equals both
  // respect default value semantics, Message.getAllFields
  // does not include fields which are not set
  // so we need to manually remove fields with default values
  // if we want to compare some Message _inside_ a Message
  def normaliseDefaultValues(message: Message): Message = {
    def go(msg: Message): Message = {
      // fields inside oneofs cannot be optional
      // since if they all have default values that would mean that
      // all the fields are set
      val oneOfFields = msg.getDescriptorForType.getOneofs.asScala
        .flatMap(_.getFields.asScala)
        .toSet

      msg.getAllFields.asScala.foldLeft(msg) {
        case (m, (f, value)) =>
          val newValue = f.getType match {
            case MESSAGE | GROUP =>
              if (!f.isRepeated) Some(go(value.asInstanceOf[Message]))
              else {
                Some(
                  value.asInstanceOf[util.List[Message]].asScala.map(go).asJava)
              }
            case _ =>
              Some(value)
                .filter(
                  v =>
                    f.isRequired ||
                      oneOfFields.contains(f) ||
                      v != f.getDefaultValue)
          }

          newValue.fold {
            m.toBuilder.clearField(f).build
          } {
            m.toBuilder.setField(f, _).build
          }
      }
    }

    go(message)
  }

  def walkMessage(path: Seq[Segment], o: Object): Option[Object] =
    if (path.isEmpty) Some(o)
    else
      path.head match {
        case Key(k) =>
          o.asInstanceOf[Message]
            .getDescriptorForType
            .getFields
            .asScala
            .find { _.getName == k }
            .flatMap { field =>
              walkMessage(path.tail, o.asInstanceOf[Message].getField(field))
            }
        case Index(i) =>
          Option(o.asInstanceOf[util.List[Object]].asScala)
            .filter(_.size > i)
            .flatMap(list => walkMessage(path.tail, list(i)))
      }

  def pathsOf(soFar: Seq[Segment], message: Message): Seq[Seq[Segment]] =
    if (message.getAllFields.isEmpty) Seq(soFar)
    else
      (for ((f: FieldDescriptor, v: AnyRef) <- message.getAllFields.asScala.toSeq)
        yield {
          val key = Key(f.getName.replace(".", "_"))

          if (!f.isRepeated) {
            f.getType match {
              case MESSAGE | GROUP =>
                pathsOf(soFar :+ key, v.asInstanceOf[Message])
              case _ => Seq(soFar :+ key)
            }
          } else {
            f.getType match {
              case MESSAGE | GROUP =>
                v.asInstanceOf[util.List[Message]]
                  .asScala
                  .zipWithIndex
                  .flatMap {
                    case (msg, i) => pathsOf(soFar :+ key :+ Index(i), msg)
                  }
              case _ =>
                (0 until v.asInstanceOf[util.List[_]].size).map(i =>
                  soFar :+ key :+ Index(i))
            }
          }
        }).flatten

  def localContext(path: Seq[Segment]): Seq[Segment] =
    if (path.nonEmpty) path.init else path

  def walk(path: Seq[Segment], tableRow: TableRow): Option[Object] = {
    def go(p: Seq[Segment], v: Object): Option[Object] =
      if (p.isEmpty) Some(v)
      else
        p.head match {
          case Key(k) =>
            v match {
              case struct: GenericData =>
                Option(struct.get(k))
                  .fold[Option[Object]](None)(go(p.tail, _))
            }
          case Index(i) =>
            v match {
              case repeated: java.util.List[_] =>
                Option(repeated)
                  .filter(_.size > i)
                  .flatMap(list => go(p.tail, list.get(i).asInstanceOf[Object]))
            }
        }

    go(path, tableRow)
  }

  // https://cloud.google.com/bigquery/docs/nested-repeated#limitations
  def pathsOf(tableRow: TableRow, maxDepth: Int = 15): Seq[Seq[Segment]] = {
    def go(soFar: Seq[Segment], depth: Int, o: Object): Seq[Seq[Segment]] =
      o match {
        case tableRow: GenericData =>
          tableRow.entrySet.asScala.toSeq.flatMap { entry =>
            entry.getValue match {
              case data: GenericData =>
                if (depth <= maxDepth)
                  go(soFar :+ Key(entry.getKey), depth + 1, data)
                else Seq.empty
              case repeated: java.util.List[_] =>
                repeated.asScala.zipWithIndex.flatMap {
                  case (r, i) =>
                    go(soFar :+ Key(entry.getKey) :+ Index(i),
                       depth + 1,
                       r.asInstanceOf[Object])
                }
              case _ => Seq(soFar :+ Key(entry.getKey))
            }
          }
        case _ => Seq(soFar)
      }

    go(Seq.empty, 0, tableRow.asInstanceOf[GenericData])
  }

  def walk(path: Seq[Segment],
           ts: util.List[TableFieldSchema]): Option[TableFieldSchema] =
    path match {
      // all positions have the same schema definition!
      case Seq() :+ Key(lastKey) :+ Index(_) =>
        ts.asScala.find(_.getName == lastKey)
      case Seq() :+ Key(key) => ts.asScala.find(_.getName == key)

      // all positions have the same schema definition!
      case Index(_) +: rest => walk(rest, ts)

      case Key(key) +: rest =>
        ts.asScala
          .find(_.getName == key)
          .flatMap(next => walk(rest, next.getFields))
    }

  def walk(path: Seq[Segment], t: TableSchema): Option[TableFieldSchema] =
    walk(path, t.getFields)

  def compareSchema[S, T](
      l: S,
      r: T
  )(implicit w: IsBigQuerySchema[S], v: IsBigQuerySchema[T]): Seq[Diff] = {
    compareFields(Seq("root"))(
      w.toTableSchema(l).getFields.asScala.toSeq,
      v.toTableSchema(r).getFields.asScala.toSeq
    )
  }

  def compareField(
      context: Seq[String]
  )(l: TableFieldSchema, r: TableFieldSchema): Seq[Diff] = {
    val diffs = Seq(
      guard(l.getMode != r.getMode, DiffMode(context, l.getMode, r.getMode)),
      guard(l.getDescription != r.getDescription,
            DiffDescription(context, l.getDescription, r.getDescription)),
      guard(
        canonicalTypeNameFor(l.getType) != canonicalTypeNameFor(r.getType),
        DiffType(context, l.getType, r.getType)
      )
    ).filter(_.isDefined)
      .map(_.get)

    diffs ++ compareFields(context)(
      safeFromJavaList(l.getFields),
      safeFromJavaList(r.getFields)
    )
  }

  def safeFromJavaList[A](l: java.util.List[A]): Seq[A] =
    Option(l).map(_.asScala.toSeq).getOrElse(Seq.empty[A])

  def compareFields(
      context: Seq[String]
  )(l: Seq[TableFieldSchema], r: Seq[TableFieldSchema]): Seq[Diff] = {
    val diff: Seq[Diff] = Seq(
      DiffFields(
        context,
        l.map(_.getName).toSet.diff(r.map(_.getName).toSet),
        r.map(_.getName).toSet.diff(l.map(_.getName).toSet)
      )
    ).filter(d => d.leftOnly.nonEmpty || d.rightOnly.nonEmpty)

    diff ++ l
      .map(_.getName)
      .toSet
      .intersect(r.map(_.getName).toSet)
      .toSeq
      .flatMap { name =>
        compareField(context :+ name)(findField(name, l), findField(name, r))
      }
  }

  def guard[A](b: Boolean, s: A): Option[A] = if (b) Some(s) else None
  def optionNonEmpty[A](s: Iterable[A]): Option[Iterable[A]] =
    guard(s.nonEmpty, s)

  def findField(name: String, fs: Seq[TableFieldSchema]): TableFieldSchema =
    Option(fs).getOrElse(List()).find(_.getName == name).get

}
