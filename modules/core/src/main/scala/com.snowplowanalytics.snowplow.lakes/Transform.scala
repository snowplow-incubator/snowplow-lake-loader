package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import io.circe.Json

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, FailureDetails, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.{Payload => BadPayload}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, FieldValue}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

object Transform {

  case class RowsWithSchema(rows: List[Row], schema: StructType)

  def eventToRow(
    processor: BadRowProcessor,
    event: Event,
    entities: NonAtomicFields
  ): Either[BadRow, Row] =
    failForResolverErrors(processor, event, entities.igluFailures).flatMap { _ =>
      (forAtomic(event), forEntities(event, entities.fields))
        .mapN { case (atomic, nonAtomic) =>
          buildRow(atomic ::: nonAtomic)
        }
        .toEither
        .leftMap { nel =>
          BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event))
        }
    }

  private def failForResolverErrors(
    processor: BadRowProcessor,
    event: Event,
    failures: List[NonAtomicFields.ColumnFailure]
  ): Either[BadRow, Unit] = {
    val schemaFailures = failures.flatMap { case NonAtomicFields.ColumnFailure(entityType, matchingKeys, failure) =>
      entityType match {
        case TabledEntity.UnstructEvent =>
          event.unstruct_event.data match {
            case Some(SelfDescribingData(schemaKey, _)) if matchingKeys.contains(schemaKey) =>
              Some(failure)
            case _ =>
              None
          }
        case TabledEntity.Context =>
          val allContexts = event.contexts.data ::: event.derived_contexts.data
          if (allContexts.exists(context => matchingKeys.contains(context.schema)))
            Some(failure)
          else
            None
      }
    }

    NonEmptyList.fromList(schemaFailures) match {
      case None => Right(())
      case Some(nel) =>
        Left(BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event)))
    }
  }

  private def buildRow(fieldValues: List[FieldValue]): Row = {
    def extractFieldValue(fv: FieldValue): Any = fv match {
      case FieldValue.NullValue => null
      case FieldValue.StringValue(v) => v
      case FieldValue.BooleanValue(v) => v
      case FieldValue.IntValue(v) => v
      case FieldValue.LongValue(v) => v
      case FieldValue.DoubleValue(v) => v
      case FieldValue.DecimalValue(v, _) => v
      case FieldValue.TimestampValue(v) => v
      case FieldValue.DateValue(v) => v
      case FieldValue.ArrayValue(vs) => vs.map(extractFieldValue)
      case FieldValue.StructValue(vs) => buildRow(vs.map(_.value))
      case FieldValue.JsonValue(v) => v.noSpaces
    }
    Row.fromSeq(fieldValues.map(extractFieldValue))
  }

  private def forAtomic(
    event: Event
  ): ValidatedNel[FailureDetails.LoaderIgluError, List[FieldValue]] = {
    val atomicJsonValues = event.atomic
    AtomicFields.static
      .map { atomicField =>
        val jsonFieldValue = atomicJsonValues.getOrElse(atomicField.name, Json.Null)
        FieldValue.cast(atomicField)(jsonFieldValue)
      }
      .sequence
      .leftMap(castErrorToLoaderIgluError(AtomicFields.schemaKey, _))
  }

  private def forEntities(
    event: Event,
    entities: List[NonAtomicFields.ColumnGroup]
  ): ValidatedNel[FailureDetails.LoaderIgluError, List[FieldValue]] =
    entities.flatMap { case NonAtomicFields.ColumnGroup(entity, field, matchingKeys, recoveries) =>
      val head = forEntity(entity, field, matchingKeys, event)
      val tail = recoveries.toList.map { case (recoveryKey, recoveryField) =>
        forEntity(entity, recoveryField, Set(recoveryKey), event)
      }
      head :: tail
    }.sequence

  private def forEntity(
    entity: TabledEntity.EntityType,
    field: Field,
    keySet: Set[SchemaKey],
    event: Event
  ): ValidatedNel[FailureDetails.LoaderIgluError, FieldValue] = {
    val result = entity match {
      case TabledEntity.UnstructEvent => forUnstruct(field, keySet, event)
      case TabledEntity.Context => forContexts(field, keySet, event)
    }
    result.leftMap(castErrorToLoaderIgluError(keySet.max, _))
  }

  private def forUnstruct(
    field: Field,
    keySet: Set[SchemaKey],
    event: Event
  ): ValidatedNel[CastError, FieldValue] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, unstructData)) if keySet.contains(schemaKey) =>
        FieldValue.cast(field)(unstructData)
      case _ =>
        Validated.Valid(FieldValue.NullValue)
    }

  private def forContexts(
    field: Field,
    keySet: Set[SchemaKey],
    event: Event
  ): ValidatedNel[CastError, FieldValue] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => keySet.contains(context.schema))

    if (matchingContexts.nonEmpty) {
      val jsonArrayWithContexts = Json.fromValues(matchingContexts.map(_.data).toVector)
      FieldValue.cast(field)(jsonArrayWithContexts)
    } else {
      Validated.Valid(FieldValue.NullValue)
    }
  }

  private def castErrorToLoaderIgluError(
    schemaKey: SchemaKey,
    castErrors: NonEmptyList[CastError]
  ): NonEmptyList[FailureDetails.LoaderIgluError] =
    castErrors.map {
      case CastError.WrongType(v, e) => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

}
