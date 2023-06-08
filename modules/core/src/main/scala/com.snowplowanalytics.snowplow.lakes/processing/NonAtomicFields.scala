package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.badrows.FailureDetails

/**
 * Describes the Field Types (not data) present in a batch of events
 *
 * @param fields
 *   field type information about each family of Iglu schema. E.g. if a batch contains versions
 *   1-0-0, 1-0-1 and 1-1-0 of a schema, they will be present as a single item of this list. If the
 *   batch also contains version 2-0-0 of that schema, it will be present as an extra item of this
 *   list.
 * @param igluFailures
 *   details of schemas that were present in the batch but could not be looked up by the Iglu
 *   resolver.
 */
private[processing] case class NonAtomicFields(fields: List[TypedTabledEntity], igluFailures: List[NonAtomicFields.ColumnFailure])

private[processing] object NonAtomicFields {

  /**
   * Describes a failure to lookup a series of Iglu schemas
   *
   * @param tabledEntity
   *   The family of iglu schemas for which the lookup was needed
   * @param versionsInBatch
   *   The schema versions for which a lookup was needed
   * @param failure
   *   Why the lookup failed
   */
  case class ColumnFailure(
    tabledEntity: TabledEntity,
    versionsInBatch: Set[SchemaSubVersion],
    failure: FailureDetails.LoaderIgluError
  )

  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]]
  ): F[NonAtomicFields] =
    entities.toList
      .traverse { case (tabledEntity, subVersions) =>
        SchemaProvider
          .fetchSchemasWithSameModel(resolver, TabledEntity.toSchemaKey(tabledEntity, subVersions.max))
          .map(TypedTabledEntity.build(tabledEntity, subVersions, _))
          .leftMap(ColumnFailure(tabledEntity, subVersions, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        NonAtomicFields(good, failures)
      }
}
