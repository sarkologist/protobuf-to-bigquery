package utils

import util.ProtobufToBigQuery._
import util.bigquery.BigQuerySchema._
import com.google.api.client.util.GenericData
import com.google.protobuf.Message
import protobufgen._
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.tagobjects.Slow
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import protobufgen.SchemaGenerators.CompiledSchema

import scala.language.existentials
import scala.jdk.CollectionConverters._

class ProtobufToBigquerySpec
    extends AnyPropSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers {
  property("min and max id are consecutive over files") {
    forAll(GraphGen.genRootNode) { node =>
      def validateMinMax(pairs: Seq[(Int, Int)]) =
        pairs.sliding(2).filter(_.size == 2).forall {
          case Seq((min1, max1), (min2, max2)) =>
            min2 == max1 + 1 && min1 <= max1 && min2 <= max2
        }
      val messageIdPairs: Seq[(Int, Int)] = node.files.flatMap { f =>
        (f.minMessageId.map((_, f.maxMessageId.get)))
      }
      val enumIdPairs: Seq[(Int, Int)] = node.files.flatMap { f =>
        (f.minEnumId.map((_, f.maxEnumId.get)))
      }
      validateMinMax(messageIdPairs) && validateMinMax(enumIdPairs)
    }
  }

  case class DisableShrink[A](a: A)

  def isTerminalPath(path: Seq[Segment]): Boolean =
    path match {
      // repeated primitive
      // non-empty message would have a subsequent Key
      // empty message has no ambiguity issue
      case _ :+ Key(_) :+ Index(_) => false
      case _                       => true
    }

  property("hit bottom implies hit potential bottom", Slow) {
    forAll(SchemaGenerators.genCompiledSchema :| "protobuf schema", workers(1)) {
      schema: CompiledSchema =>
        schema.rootNode.maxMessageId.foreach { maxMsgId =>
          forAll(Gen.choose(0, maxMsgId).map(DisableShrink[Int]) :| "msgId") {
            case DisableShrink(msgId) =>
              val node = schema.rootNode.messagesById(msgId)
              forAll(
                GenData
                  .genParsedMessage(schema, node)
              ) { msg =>
                !hitBottom(msg) || hitPotentialBottom(msg.getDescriptorForType)
              }
          }
        }
    }
  }

  property("derived TableRow is compatible with derived schema", Slow) {
    forAll(SchemaGenerators.genCompiledSchema :| "protobuf schema", workers(1)) {
      schema: CompiledSchema =>
        schema.rootNode.maxMessageId.foreach { maxMsgId =>
          forAll(Gen.choose(0, maxMsgId).map(DisableShrink[Int]) :| "msgId") {
            case DisableShrink(msgId) =>
              val node = schema.rootNode.messagesById(msgId)
              forAll(GenData.genParsedMessage(schema, node)) { msg =>
                val schema   = makeTableSchema(msg.getDescriptorForType)
                val tablerow = makeTableRow { case (_, b) => b }(msg)

                for (path <- pathsOf(tablerow)) {
                  val assertion = walk(path, schema).nonEmpty

                  assert(assertion)
                }
              }
          }
        }
    }
  }

  property("default values are correct", Slow) {
    forAll(SchemaGenerators.genCompiledSchema :| "protobuf schema",
           workers(1),
           minSuccessful(100)) { schema: CompiledSchema =>
      schema.rootNode.maxMessageId.foreach { maxMsgId =>
        forAll(Gen.choose(0, maxMsgId).map(DisableShrink[Int]) :| "msgId") {
          case DisableShrink(msgId) =>
            val node = schema.rootNode.messagesById(msgId)
            forAll(GenData.genParsedMessage(schema, node)) { msg =>
              val filledMsg =
                fillInDefaultValues(msg)
              val normalisedFilledMsg = normaliseDefaultValues(filledMsg)
              val normalisedMsg       = normaliseDefaultValues(msg)

              val assertion = normalisedFilledMsg == normalisedMsg

              assert(assertion)
            }
        }
      }
    }
  }

  property("protobuf Message to TableRow does not add _set when not necessary",
           Slow) {
    forAll(SchemaGenerators.genCompiledSchema :| "protobuf schema",
           workers(1),
           minSuccessful(100)) { schema: CompiledSchema =>
      schema.rootNode.maxMessageId.foreach { maxMsgId =>
        forAll(Gen.choose(0, maxMsgId).map(DisableShrink[Int]) :| "msgId") {
          case DisableShrink(msgId) =>
            val node = schema.rootNode.messagesById(msgId)
            forAll(
              GenData
                .genParsedMessage(schema, node)
                .suchThat(pathsOf(_).exists(isTerminalPath))) { msg =>
              forAll(
                Gen.oneOf(pathsOf(msg)
                  .filter(isTerminalPath)
                  .map(DisableShrink[Seq[Segment]])) :| "path") {
                case DisableShrink(path) =>
                  val row = makeTableRow { case (_, b) => b }(msg)

                  val leafParentMessage =
                    walk(localContext(path), msg).get.asInstanceOf[Message]

                  val leafObject = walk(path, msg).get

                  val leafMaybeMessage = leafObject match {
                    case m: Message => Some(m)
                    case _          => None
                  }

                  val leafColumns = walk(path, row).get

                  val leafHasAlwaysPresentFields =
                    leafMaybeMessage.exists(m =>
                      hasAlwaysPresentFields(m.getDescriptorForType))

                  val leafHasSetColumn = leafColumns match {
                    case record: GenericData => record.containsKey("_set")
                    case _                   => false
                  }

                  val leafIsPrimitive = leafObject match {
                    case _: Message           => false
                    case _: java.util.List[_] => false
                    case _                    => true
                  }

                  val leafIsRequired =
                    leafParentMessage.getAllFields.asScala.keys
                      .find(field =>
                        path.lastOption match {
                          case Some(Key(name)) => field.getName == name
                          case _               => false
                      })
                      .exists(_.isRequired)

                  val atRoot = localContext(path) == path

                  val assertion =
                    !(atRoot ||
                      leafIsPrimitive ||
                      leafIsRequired ||
                      leafHasAlwaysPresentFields) ==
                      leafHasSetColumn

                  assert(assertion)
              }
            }
        }
      }
    }
  }

  property("protobuf Message to TableRow is locally unambiguous", Slow) {
    forAll(SchemaGenerators.genCompiledSchema :| "protobuf schema",
           workers(1),
           minSuccessful(100)) { schema: CompiledSchema =>
      schema.rootNode.maxMessageId.foreach { maxMsgId =>
        forAll(Gen.choose(0, maxMsgId).suchThat(_ >= 0) :| "msgId") { msgId =>
          val node = schema.rootNode.messagesById(msgId)

          forAll((for {
            msg1 <- GenData.genParsedMessage(schema, node)
            msg2 <- GenData.genParsedMessage(schema, node)
          } yield (msg1, msg2)) :| "generated message pair") {
            case (msgOne, msgTwo) =>
              val tr1 = makeTableRow { case (_, b) => b }(msgOne)
              val tr2 = makeTableRow { case (_, b) => b }(msgTwo)

              forAll(Gen.oneOf(pathsOf(msgOne) ++ pathsOf(msgTwo)) :| "path") {
                path =>
                  for {
                    walk1 <- walk(path, normaliseDefaultValues(msgOne))
                    walk2 <- walk(path, normaliseDefaultValues(msgTwo))
                  } yield {
                    val walkLocal1 =
                      walk(localContext(path), tr1)
                        .map(normaliseEmptyGenericData)
                    val walkLocal2 =
                      walk(localContext(path), tr2)
                        .map(normaliseEmptyGenericData)

                    if (path != localContext(path) && walk1 != walk2) {
                      walkLocal1 should not be walkLocal2
                    }
                  }
              }
          }
        }
      }
    }
  }

}
