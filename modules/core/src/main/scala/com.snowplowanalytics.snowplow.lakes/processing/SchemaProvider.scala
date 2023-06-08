package com.snowplowanalytics.snowplow.lakes.processing

import cats.data.{EitherT, NonEmptyList}
import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import scala.math.Ordered._

private[processing] object SchemaProvider {

  private def getSchema[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, SchemaWithKey] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey))
                .leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield SchemaWithKey(schemaKey, schema)

  def fetchSchemasWithSameModel[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, NonEmptyList[SchemaWithKey]] =
    for {
      schemaKeys <- EitherT(resolver.listSchemasLike(schemaKey))
                      .leftMap(resolverFetchBadRow(schemaKey.vendor, schemaKey.name, schemaKey.format, schemaKey.version.model))
                      .map(_.schemas)
      schemaKeys <- EitherT.rightT[F, FailureDetails.LoaderIgluError](schemaKeys.filter(_ < schemaKey))
      topSchema <- getSchema(resolver, schemaKey)
      lowerSchemas <- schemaKeys.filter(_ < schemaKey).traverse(getSchema(resolver, _))
    } yield NonEmptyList(topSchema, lowerSchemas)

  private def resolverFetchBadRow(
    vendor: String,
    name: String,
    format: String,
    model: Int
  )(
    e: ClientError.ResolutionError
  ): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.SchemaListNotFound(SchemaCriterion(vendor = vendor, name = name, format = format, model = model), e)

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

}
