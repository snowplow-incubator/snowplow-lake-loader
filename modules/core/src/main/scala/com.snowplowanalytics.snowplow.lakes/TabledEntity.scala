package com.snowplowanalytics.snowplow.lakes

import cats.Monoid
import cats.implicits._

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

case class TabledEntity(
  entityType: TabledEntity.EntityType,
  vendor: String,
  schemaName: String,
  model: Int
)

object TabledEntity {
  sealed trait EntityType
  case object UnstructEvent extends EntityType
  case object Context extends EntityType

  def forEvent(event: Event): Map[TabledEntity, Set[SchemaKey]] = {
    val ue = event.unstruct_event.data.map(sdj => Map(forUnstructEvent(sdj.schema) -> Set(sdj.schema)))

    val contexts = (event.contexts.data ++ event.derived_contexts.data).map { sdj =>
      Map(forContext(sdj.schema) -> Set(sdj.schema))
    }

    Monoid[Map[TabledEntity, Set[SchemaKey]]].combineAll(contexts ++ ue)
  }

  private def forUnstructEvent(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(UnstructEvent, schemaKey.vendor, schemaKey.name, schemaKey.version.model)

  private def forContext(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(Context, schemaKey.vendor, schemaKey.name, schemaKey.version.model)
}
