package com.snowplowanalytics.snowplow.lakes.processing

import cats.Monoid
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

/**
 * Identifier for a family of versioned Snowplow entities which are treated as a common entity when
 * writing to the table
 *
 * E.g. unstruct events with types 1-0-0, 1-0-1, and 1-1-0 are treated as the same TabledEntity
 *
 * @param entityType
 *   whether this is a unstruct event or context
 * @param vendor
 *   the Iglu schema vendor
 * @param schemaName
 *   the Iglu schema name
 * @param model
 *   the Iglu schema model number, i.e. 1-*-*
 */
private[processing] case class TabledEntity(
  entityType: TabledEntity.EntityType,
  vendor: String,
  schemaName: String,
  model: Int
)

private[processing] object TabledEntity {
  sealed trait EntityType
  case object UnstructEvent extends EntityType
  case object Context extends EntityType

  /**
   * Extracts which TabledEntities will need to be included in the batch for this event, and the
   * corresponding sub-versions (e.g. *-1-1)
   */
  def forEvent(event: Event): Map[TabledEntity, Set[SchemaSubVersion]] = {
    val ue = event.unstruct_event.data.map(sdj => Map(forUnstructEvent(sdj.schema) -> Set(keyToSubVersion(sdj.schema))))

    val contexts = (event.contexts.data ++ event.derived_contexts.data).map { sdj =>
      Map(forContext(sdj.schema) -> Set(keyToSubVersion(sdj.schema)))
    }

    Monoid[Map[TabledEntity, Set[SchemaSubVersion]]].combineAll(contexts ++ ue)
  }

  def toSchemaKey(te: TabledEntity, subver: SchemaSubVersion): SchemaKey = {
    val ver = SchemaVer.Full(te.model, subver._1, subver._2)
    SchemaKey(vendor = te.vendor, name = te.schemaName, format = "jsonschema", version = ver)
  }

  private def keyToSubVersion(key: SchemaKey): SchemaSubVersion = (key.version.revision, key.version.addition)

  private def forUnstructEvent(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(UnstructEvent, schemaKey.vendor, schemaKey.name, schemaKey.version.model)

  private def forContext(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(Context, schemaKey.vendor, schemaKey.name, schemaKey.version.model)
}
