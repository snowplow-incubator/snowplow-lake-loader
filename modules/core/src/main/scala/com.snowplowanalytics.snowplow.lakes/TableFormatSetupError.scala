/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import org.apache.iceberg.exceptions.{
  BadRequestException,
  ForbiddenException => IcebergForbiddenException,
  NoSuchIcebergTableException,
  NoSuchNamespaceException,
  NoSuchWarehouseException,
  NotAuthorizedException,
  NotFoundException => IcebergNotFoundException,
  RESTException
}

import org.apache.iceberg.rest.auth.OAuth2Properties

import org.apache.spark.sql.delta.DeltaAnalysisException

import java.net.UnknownHostException

object TableFormatSetupError {

  // Check if given exception is specific to iceberg format
  def check(target: Config.Target): PartialFunction[Throwable, String] =
    target match {
      case _: Config.Delta => Delta.check.unlift
      case c: Config.Iceberg =>
        c.catalog match {
          case _: Config.IcebergCatalog.Glue   => IcebergGlue.check.unlift
          case _: Config.IcebergCatalog.Rest   => IcebergRest.check.unlift
          case _: Config.IcebergCatalog.Hadoop => PartialFunction.empty
        }
      case _ => PartialFunction.empty
    }

  object Delta {
    def check: Throwable => Option[String] = {
      case e: DeltaAnalysisException if e.errorClass == Some("DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION") =>
        Some("Destination not empty and not a Delta table")
      case _ => None
    }
  }

  object IcebergGlue {
    def check: Throwable => Option[String] = {
      case _: NoSuchIcebergTableException =>
        // Table exists but not in Iceberg format
        Some("Target table is not an Iceberg table")
      case e: IcebergNotFoundException =>
        // Glue catalog does not exist
        Some(e.getMessage)
      case e: IcebergForbiddenException =>
        // No permission to create a table in Glue catalog
        Some(e.getMessage)
      case _ => None
    }
  }

  object IcebergRest {
    def check: Throwable => Option[String] = {
      case e: IcebergForbiddenException =>
        if (checkS3RestCatalogPermissionError(e) || checkFileReadPermissionError(e))
          Some("IAM role of the REST catalog is missing permissions")
        else if (checkRestCatalogRolePermissionError(e))
          Some("REST catalog role is missing permissions")
        else if (checkRestSpecPermissionError(e))
          Some("Missing privileges on the catalog, schema or table")
        else
          None
      case e: NoSuchNamespaceException =>
        Some(e.getMessage)
      case e: BadRequestException if messageContains(e, "CATALOG_DOES_NOT_EXIST") =>
        // Databricks responds with HTTP 400 when the catalog (warehouse) does not exist
        Some("Unable to find given catalog")
      case e: BadRequestException if messageContains(e, "is not an Iceberg compatible table") =>
        // Databricks responds with HTTP 400 when the table exists but is not in Iceberg format
        Some("Target table is not an Iceberg table")
      case e: BadRequestException =>
        extractOauthErrorType(e).map(oauthErrorMessage)
      case e: NotAuthorizedException =>
        extractOauthErrorType(e).map(oauthErrorMessage)
      case _: NoSuchWarehouseException =>
        Some("Unable to find given catalog")
      case e: RESTException if isTransientHttpStatus(e) =>
        // Transient status (timeout / rate limiting / gateway error); must be retried, not classified as a setup error
        None
      case e: RESTException if messageContains(e, "Unable to process") =>
        if (messageContains(e, "Service: S3, Status Code: 301"))
          Some(
            "REST catalog returned an error. Check your REST catalog configuration. A possible cause is invalid S3 bucket region for this catalog"
          )
        else if (messageContains(e, "exceeds the maximum allowed size"))
          Some(
            "Table metadata exceeds the REST catalog's maximum allowed size. Expire old snapshots and compact the table to reduce its metadata size."
          )
        else if (messageContains(e, "Make sure it references valid GCP project"))
          Some("Unable to find the given GCP project for the catalog")
        else
          Some("REST catalog returned an error. Check your REST catalog configuration")
      case e: RESTException if Option(e.getCause).exists(_.isInstanceOf[UnknownHostException]) =>
        Some("REST catalog URI isn't reachable")
      case e: RESTException if Option(e.getCause).exists(c => messageContains(c, "Target host is not specified")) =>
        // The http client rejects a catalog URI without a scheme or host before sending any request
        Some("REST catalog URI is invalid. Check your REST catalog configuration")
      case _ => None
    }

    private def messageContains(t: Throwable, fragment: String): Boolean =
      Option(t.getMessage).exists(_.contains(fragment))

    private val transientHttpStatuses = Set(408, 429, 502, 504)

    private val httpStatusPattern = """\(code: (\d+)""".r.unanchored

    /**
     * HTTP statuses that Iceberg's DefaultErrorHandler does not map to a dedicated exception --
     * they surface as a generic RESTException carrying "Unable to process (code: <n>, ...)" -- and
     * that are transient rather than setup problems: 408 (request timeout), 429 (rate limited), 502
     * (bad gateway), 504 (gateway timeout). These must fall through to the transient retry path,
     * not be reported as setup errors.
     */
    private def isTransientHttpStatus(e: RESTException): Boolean =
      Option(e.getMessage) match {
        case Some(httpStatusPattern(code)) => transientHttpStatuses.contains(code.toInt)
        case _                             => false
      }

    /**
     * "Not authorized to make this request" is the REST catalog spec's canonical 403 message,
     * returned by spec-compliant catalogs on a permission failure. Databricks Unity Catalog returns
     * it for any missing privilege (USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY). BigLake
     * prefixes its "Permission '<perm>' denied on project '<id>'" detail with it.
     */
    private def checkRestSpecPermissionError(exception: IcebergForbiddenException): Boolean =
      messageContains(exception, "Not authorized to make this request")

    private def checkFileReadPermissionError(exception: IcebergForbiddenException): Boolean = {
      val badRequestPattern = """.*Forbidden: Failed to read file.*""".r
      badRequestPattern.matches(exception.getMessage)
    }

    private def checkS3RestCatalogPermissionError(exception: IcebergForbiddenException): Boolean = {
      val badRequestPattern = """.*is not authorized to perform.*no identity-based policy allows.*""".r
      badRequestPattern.matches(exception.getMessage)
    }

    private def checkRestCatalogRolePermissionError(exception: IcebergForbiddenException): Boolean = {
      val badRequestPattern = """.*Principal.*with activated PrincipalRoles.*is not authorized for op.*""".r
      badRequestPattern.matches(exception.getMessage)
    }

    private def extractOauthErrorType(exception: RESTException): Option[String] = {
      val errorTypes = List(
        OAuth2Properties.INVALID_REQUEST_ERROR,
        OAuth2Properties.INVALID_CLIENT_ERROR,
        OAuth2Properties.INVALID_GRANT_ERROR,
        OAuth2Properties.UNAUTHORIZED_CLIENT_ERROR,
        OAuth2Properties.UNSUPPORTED_GRANT_TYPE_ERROR,
        OAuth2Properties.INVALID_SCOPE_ERROR
      )
      val badRequestPattern    = """.*Malformed request: (\w+): .*""".r
      val notAuthorizedPattern = """.*Not authorized: (\w+): .*""".r

      val errorType = exception.getMessage match {
        case badRequestPattern(errorType)    => Some(errorType)
        case notAuthorizedPattern(errorType) => Some(errorType)
        case _                               => None
      }
      errorType.filter(errorTypes.contains(_))
    }

    private def oauthErrorMessage(errorType: String): String =
      s"OAuth error from REST Iceberg catalog: $errorType"
  }
}
