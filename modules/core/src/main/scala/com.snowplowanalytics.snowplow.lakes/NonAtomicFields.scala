package com.snowplowanalytics.snowplow.lakes

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Migrations, Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data => SdkData, SnowplowEvent}

import scala.math.abs

case class NonAtomicFields(fields: List[NonAtomicFields.ColumnGroup], igluFailures: List[NonAtomicFields.ColumnFailure])

object NonAtomicFields {
  case class ColumnGroup(
    entity: TabledEntity.EntityType,
    field: Field,
    matchingKeys: Set[SchemaKey],
    recoveries: Map[SchemaKey, Field]
  )
  case class ColumnFailure(
    entity: TabledEntity.EntityType,
    matchingKeys: Set[SchemaKey],
    failure: FailureDetails.LoaderIgluError
  )

  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaKey]]
  ): F[NonAtomicFields] =
    entities.toList
      .traverse { case (tabledEntity, matchingKeys) =>
        SchemaProvider
          .fetchSchemasWithSameModel(resolver, matchingKeys.max)
          .map(buildColumnGroup(tabledEntity, matchingKeys, _))
          .leftMap(ColumnFailure(tabledEntity.entityType, matchingKeys, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        NonAtomicFields(good, failures)
      }

  private def buildColumnGroup(
    tabledEntity: TabledEntity,
    keySet: Set[SchemaKey],
    schemas: NonEmptyList[SchemaProvider.SchemaWithKey]
  ): ColumnGroup = {
    // Schemas need to be ordered by key to merge in correct order.
    val NonEmptyList(root, tail) = schemas.sorted
    val columnGroup = ColumnGroup(tabledEntity.entityType, fieldFromSchema(tabledEntity, root.schema), Set(root.schemaKey), Map.empty)
    tail
      .map(schemaWithKey => (fieldFromSchema(tabledEntity, schemaWithKey.schema), schemaWithKey.schemaKey))
      .foldLeft(columnGroup) { case (columnGroup, (field, schemaKey)) =>
        Migrations.mergeSchemas(columnGroup.field, field) match {
          case Left(_) =>
            if (keySet.contains(schemaKey)) {
              val hash = abs(field.hashCode())
              // typedField always has a single element in matchingKeys
              val recoverPoint = schemaKey.version.asString.replaceAll("-", "_")
              val newName = s"${field.name}_recovered_${recoverPoint}_$hash"
              columnGroup.copy(recoveries = columnGroup.recoveries + (schemaKey -> field.copy(name = newName)))
            } else {
              // do not create a recovered column if that type were not in the batch
              columnGroup
            }
          case Right(mergedField) =>
            columnGroup.copy(field = mergedField, matchingKeys = columnGroup.matchingKeys + schemaKey)
        }
      }
  }

  private def fieldFromSchema(tabledEntity: TabledEntity, schema: Schema): Field = {
    val sdkEntityType = tabledEntity.entityType match {
      case TabledEntity.UnstructEvent => SdkData.UnstructEvent
      case TabledEntity.Context => SdkData.Contexts(SdkData.CustomContexts)
    }
    val fieldName = SnowplowEvent.transformSchema(sdkEntityType, tabledEntity.vendor, tabledEntity.schemaName, tabledEntity.model)

    Field.normalize {
      tabledEntity.entityType match {
        case TabledEntity.UnstructEvent =>
          Field.build(fieldName, schema, enforceValuePresence = false)
        case TabledEntity.Context =>
          Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
      }
    }
  }
}
