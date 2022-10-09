package protobufgen

import com.google.protobuf.{Message, TextFormat => GTextFormat}
import org.scalacheck.Gen
import protobufgen.SchemaGenerators.CompiledSchema
import scalapb.compiler.FunctionalPrinter

import scala.jdk.CollectionConverters._

object GenData {
  import GenTypes._
  import Nodes._
  import org.scalacheck.Gen._

  sealed trait ProtoValue
  case class PrimitiveValue(value: String) extends ProtoValue
  case class EnumValue(value: String)      extends ProtoValue
  case class MessageValue(values: Seq[(String, ProtoValue)])
      extends ProtoValue {
    def toAscii: String =
      printAscii(new FunctionalPrinter()).result()

    def printAscii(printer: FunctionalPrinter): FunctionalPrinter = {
      values.foldLeft(printer) {
        case (printer, (name, PrimitiveValue(value))) =>
          printer.add(s"$name: $value")
        case (printer, (name, EnumValue(value))) =>
          printer.add(s"$name: $value")
        case (printer, (name, mv: MessageValue)) =>
          printer
            .add(s"$name: <")
            .indent
            .call(mv.printAscii)
            .outdent
            .add(">")
      }
    }
  }

  private def genMessageValue(
      rootNode: RootNode,
      message: MessageNode,
      depth: Int = 0
  ): Gen[MessageValue] = {
    def genFieldValueByOptions(
        field: FieldNode): Gen[Seq[(String, ProtoValue)]] = sized { s =>
      def genCount: Gen[Int] = field.fieldOptions.modifier match {
        case FieldModifier.OPTIONAL =>
          if (depth > 3) Gen.const(0)
          // If a one of, we already considered not providing a value,
          // so we always return 1
          else if (field.oneOfGroup.isOneof) Gen.const(1)
          else Gen.frequency(5 -> 0, 1 -> 1)
        case FieldModifier.REQUIRED =>
          Gen.const(1)
        case FieldModifier.REPEATED =>
          // TODO(nadavsr): work on larger messages, limit total field count.
          if (depth > 3) Gen.const(0)
          else Gen.frequency(5 -> 0, 1 -> (((s - 2 * depth) max 0) min 2))
      }

      def genSingleFieldValue(fieldType: ProtoType): Gen[ProtoValue] =
        fieldType match {
          case Primitive(_, genValue, _, _) =>
            genValue.map(v => PrimitiveValue(v))
          case EnumReference(id) =>
            oneOf(rootNode.enumsById(id).values.map(_._1)).map(v =>
              EnumValue(v))
          case MessageReference(id) =>
            genMessageValue(rootNode, rootNode.messagesById(id), depth + 1)
          case MapType(keyType, valueType) =>
            for {
              k <- genSingleFieldValue(keyType)
              v <- genSingleFieldValue(valueType)
            } yield MessageValue(Seq("key" -> k, "value" -> v))
        }

      for {
        count <- genCount
        result <- Gen.listOfN(
          count,
          genSingleFieldValue(field.fieldType).map(field.name -> _))
      } yield result
    }

    def oneofGroups: Map[GraphGen.OneOfGrouping, Seq[FieldNode]] =
      message.fields.groupBy(_.oneOfGroup) - GraphGen.NotInOneof

    // chooses Some(field) from a oneof group, or None.
    def chooseFieldFromGroup(l: Seq[FieldNode]): Gen[Option[FieldNode]] =
      Gen.frequency(1 -> Gen.oneOf(l.map(Some(_))), 1 -> None)

    val fieldGens: Seq[Gen[Seq[(String, ProtoValue)]]] =
      message.fields.collect {
        case field if !field.oneOfGroup.isOneof =>
          genFieldValueByOptions(field)
      }

    // Chooses at most one field from each one of and generates a value for it.
    val oneofGens: Seq[Gen[Seq[(String, ProtoValue)]]] = oneofGroups.values
      .map(group =>
        chooseFieldFromGroup(group).flatMap {
          case None        => Gen.const(Seq())
          case Some(field) => genFieldValueByOptions(field)
      })
      .toSeq

    val x: Seq[Gen[Seq[(String, ProtoValue)]]] = fieldGens ++ oneofGens

    Gen.sequence(x).map(s => MessageValue(s.asScala.toSeq.flatten))
  }

  def genMessageId(rootNode: RootNode): Gen[Int] =
    Gen.choose(0, rootNode.maxMessageId.get)

  def genMessageNode(rootNode: RootNode): Gen[MessageNode] =
    for {
      messageId <- genMessageId(rootNode)
    } yield rootNode.messagesById(messageId)

  // a parsed message cannot distinguish optional fields not being set
  // versus it being set to a default value
  def genParsedMessage(schema: CompiledSchema,
                       messageNode: MessageNode): Gen[Message] =
    genMessage(schema, messageNode).map(m =>
      m.getParserForType.parseFrom(m.toByteArray))

  def genMessage(schema: CompiledSchema,
                 messageNode: MessageNode): Gen[Message] =
    for {
      messageValue <- genMessageValue(schema.rootNode, messageNode)
    } yield buildMessage(schema, messageNode, messageValue)

  def buildMessage(schema: CompiledSchema,
                   messageNode: Nodes.MessageNode,
                   messageValue: GenData.MessageValue): Message = {
    // Ascii to binary is the same.
    val messageAscii = messageValue.toAscii
    val builder      = schema.javaBuilder(messageNode)
    GTextFormat.merge(messageAscii, builder)
    builder.build
  }
}
