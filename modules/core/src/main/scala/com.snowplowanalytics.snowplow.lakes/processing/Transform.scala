/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.implicits._
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import io.circe.Json

import org.apache.spark.sql.Row

import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, FailureDetails, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.{Payload => BadPayload}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.iglu.schemaddl.parquet.{CastError, FieldValue}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

private[processing] object Transform {

  /**
   * Transform a Snowplow Event to a Spark Row
   *
   * @param processor
   *   Details about this loader, only used for generating a bad row
   * @param event
   *   The Snowplow Event received from the stream
   * @param batchInfo
   *   Pre-calculated Iglu types for a batch of events, used to guide the transformation
   * @return
   *   A Spark Row if iglu schemas were successfully resolved for this event, and the event's
   *   entities can be cast to the schema-ddl types. Otherwise a bad row.
   */
  def eventToRow(
    processor: BadRowProcessor,
    event: Event,
    batchInfo: NonAtomicFields
  ): Either[BadRow, Row] =
    failForResolverErrors(processor, event, batchInfo.igluFailures).flatMap { _ =>
      (forAtomic(event), forEntities(event, batchInfo.fields))
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
    val schemaFailures = failures.flatMap { case NonAtomicFields.ColumnFailure(tabledEntity, versionsInBatch, failure) =>
      tabledEntity.entityType match {
        case TabledEntity.UnstructEvent =>
          event.unstruct_event.data match {
            case Some(SelfDescribingData(schemaKey, _)) if keyMatches(tabledEntity, versionsInBatch, schemaKey) =>
              Some(failure)
            case _ =>
              None
          }
        case TabledEntity.Context =>
          val allContexts = event.contexts.data ::: event.derived_contexts.data
          if (allContexts.exists(context => keyMatches(tabledEntity, versionsInBatch, context.schema)))
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
    entities: List[TypedTabledEntity]
  ): ValidatedNel[FailureDetails.LoaderIgluError, List[FieldValue]] =
    entities.flatMap { case TypedTabledEntity(entity, field, subVersions, recoveries) =>
      val head = forEntity(entity, field, subVersions, event)
      val tail = recoveries.toList.map { case (recoveryVersion, recoveryField) =>
        forEntity(entity, recoveryField, Set(recoveryVersion), event)
      }
      head :: tail
    }.sequence

  private def forEntity(
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[FailureDetails.LoaderIgluError, FieldValue] = {
    val result = te.entityType match {
      case TabledEntity.UnstructEvent => forUnstruct(te, field, subVersions, event)
      case TabledEntity.Context => forContexts(te, field, subVersions, event)
    }
    result.leftMap { failure =>
      val schemaKey = TabledEntity.toSchemaKey(te, subVersions.max)
      castErrorToLoaderIgluError(schemaKey, failure)
    }
  }

  private def forUnstruct(
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[CastError, FieldValue] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, unstructData)) if keyMatches(te, subVersions, schemaKey) =>
        FieldValue.cast(field)(unstructData)
      case _ =>
        Validated.Valid(FieldValue.NullValue)
    }

  private def forContexts(
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[CastError, FieldValue] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => keyMatches(te, subVersions, context.schema))

    if (matchingContexts.nonEmpty) {
      val jsonArrayWithContexts = Json.fromValues(matchingContexts.map(jsonForContext).toVector)
      FieldValue.cast(field)(jsonArrayWithContexts)
    } else {
      Validated.Valid(FieldValue.NullValue)
    }
  }

  private def keyMatches(
    te: TabledEntity,
    subVersions: Set[SchemaSubVersion],
    schemaKey: SchemaKey
  ): Boolean =
    (schemaKey.vendor === te.vendor) &&
      (schemaKey.name === te.schemaName) &&
      (schemaKey.version.model === te.model) &&
      subVersions.exists { case (revision, addition) =>
        schemaKey.version.revision === revision && schemaKey.version.addition === addition
      }

  private def castErrorToLoaderIgluError(
    schemaKey: SchemaKey,
    castErrors: NonEmptyList[CastError]
  ): NonEmptyList[FailureDetails.LoaderIgluError] =
    castErrors.map {
      case CastError.WrongType(v, e) => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def jsonForContext(sdd: SelfDescribingData[Json]): Json =
    sdd.data.mapObject { obj =>
      // Our special key takes priority over a key of the same name in the object
      obj.add("_schema_version", Json.fromString(sdd.schema.version.asString))
    }

}
