package util

import com.google.protobuf.Descriptors.FieldDescriptor.Type.{GROUP, MESSAGE}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Message
import scalaz.{Functor, Yoneda}

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object Protobuf {
  def foldDescriptor[A](
      base: FieldDescriptor => A,
      recurse: List[(A, FieldDescriptor)] => A,
      maxDepth: Int = Integer.MAX_VALUE)(descriptor: Descriptor): A = {
    def go(d: Descriptor, depth: Int): A = {
      val fields =
        d.getFields.asScala.foldLeft(List.empty[(A, FieldDescriptor)]) {
          case (as, f) =>
            (f.getType match {
              case MESSAGE | GROUP if depth <= maxDepth =>
                go(f.getMessageType, depth + 1) -> f
              case _ => base(f) -> f
            }) :: as
        }

      recurse(fields)
    }

    go(descriptor, 0)
  }

  def lift[A](fieldDescriptor: FieldDescriptor, value: AnyRef): Repeated[A] =
    if (fieldDescriptor.isRepeated)
      Many(value.asInstanceOf[util.List[A]].asScala)
    else One(value.asInstanceOf[A])

  /**
  - recursively traverse `Message`, producing `A`
  - Yoneda is for efficient `.map`-ing.
    it composes mapped functions without applying until `Yoneda.run` is called
  - note that this is a *paramorphism* instead of just a *catamorphism*, i.e.
    each recursive step keeps the `Message` value at that level.
    this is helpful e.g. to tell at runtime if the `Message` is a leaf
   */
  def foldMessage[A](
      recurse: Seq[(Yoneda[Repeated, (A, Message)], FieldDescriptor)] => A,
      base: (AnyRef, FieldDescriptor) => A)(message: Message): A = {
    def go(msg: Message): A = {
      val presentFields =
        msg.getAllFields.asScala
          .foldLeft(
            Seq.empty[(Yoneda[Repeated, (A, Message)], FieldDescriptor)]) {
            case (fields, (f, value)) =>
              fields :+ (f.getType match {
                case MESSAGE | GROUP =>
                  Yoneda(lift[Message](f, value)).map(m => go(m) -> m)
                case _ =>
                  Yoneda(lift[AnyRef](f, value)).map(r => base(r, f) -> msg)

              }) -> f
          }

      val allFields = defaultValues(msg)().foldLeft(presentFields) {
        case (fields, (f: FieldDescriptor, value)) =>
          fields :+ Yoneda(lift[AnyRef](f, value))
            .map(v => base(v, f) -> msg) -> f
      }

      recurse(allFields)
    }

    go(message)
  }

  /**
   the protobuf `Message` type does not represent fields whose values are default
   */
  def defaultValues(message: Message)(presentFields: Set[FieldDescriptor] =
                                        message.getAllFields.asScala.keys.toSet)
    : Set[(FieldDescriptor, AnyRef)] = {
    val absentFields         = message.getDescriptorForType.getFields.asScala.toSet -- presentFields
    val oneOfFieldsOfMessage = oneOfFields(message.getDescriptorForType)

    // oneof fields don't have default values
    (absentFields -- oneOfFieldsOfMessage)
      .map { field =>
        if (!field.isRepeated)
          (field.getType match {
            case MESSAGE | GROUP => None
            case _ =>
              Some(field.getDefaultValue)
          }).map { defVal =>
            field -> defVal
          } else None
      }
      .collect { case Some(pair) => pair }
  }


  /**
   *  oneof fields don't have default values
   */
  def oneOfFields(descriptor: Descriptor): Set[FieldDescriptor] = {
    descriptor.getOneofs.asScala
      .flatMap(_.getFields.asScala)
      .toSet
  }

  /**
   * all protobuf field values can be repeated
   */
  sealed trait Repeated[A] {
    def underlying: AnyRef
  }
  case class One[A](a: A) extends Repeated[A] {
    def underlying: AnyRef = a.asInstanceOf[AnyRef]
  }
  // JavaConverters returns mutable.Buffer
  case class Many[A](as: mutable.Buffer[A]) extends Repeated[A] {
    def underlying: AnyRef            = cast(as.asJava)
    def cast(x: util.List[A]): AnyRef = x.asInstanceOf[AnyRef]
  }

  implicit def repeatedFunctor: Functor[Repeated] = new Functor[Repeated] {
    override def map[A, B](fa: Repeated[A])(f: A => B): Repeated[B] =
      fa match {
        case One(a)   => One(f(a))
        case Many(as) => Many(as.map(f))
      }
  }

}
