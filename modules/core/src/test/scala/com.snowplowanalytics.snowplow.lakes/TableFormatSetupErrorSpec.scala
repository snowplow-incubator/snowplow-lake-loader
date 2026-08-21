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

import org.apache.iceberg.rest.ErrorHandlers
import org.apache.iceberg.rest.responses.ErrorResponse
import org.specs2.Specification

class TableFormatSetupErrorSpec extends Specification {
  def is = s2"""
  TableFormatSetupError.IcebergRest.check should:
    not classify a rate-limiting 429 from the REST catalog as a setup error $e1
    not classify a transient 408 / 502 / 504 from the REST catalog as a setup error $e2
    still classify a non-transient "Unable to process" error as a setup error $e3
    give a specific message when the catalog rejects a write for metadata size $e4
    classify a BigLake permission denial as a setup error via the shared REST-spec 403 message $e5
    give a specific message for a BigLake project-not-found error $e6
  """

  def e1 = TableFormatSetupError.IcebergRest.check(restException(429, "")) must beNone

  def e2 =
    (TableFormatSetupError.IcebergRest.check(restException(408, "")) must beNone) and
      (TableFormatSetupError.IcebergRest.check(restException(502, "")) must beNone) and
      (TableFormatSetupError.IcebergRest.check(restException(504, "")) must beNone)

  // 405 falls through to the generic "Unable to process" RESTException but is not transient, so it
  // is still a setup error. 500 is deliberately not used here -- it maps to ServiceFailureException
  // and never reaches this arm.
  def e3 = TableFormatSetupError.IcebergRest.check(restException(405, "method not allowed")) must beSome

  // A 422 "metadata too large" carries "Unable to process" in the message, so it would otherwise land
  // on the generic "check your REST catalog configuration" fallback. It should get its own actionable
  // message instead.
  def e4 =
    TableFormatSetupError.IcebergRest.check(
      restException(422, "Write operation failed: Table metadata file gs://b/x.metadata.json exceeds the maximum allowed size.")
    ) must beSome(
      "Table metadata exceeds the REST catalog's maximum allowed size. Expire old snapshots and compact the table to reduce its metadata size."
    )

  // BigLake's 403 leads with "Not authorized to make this request" -- the REST spec's canonical 403
  // message -- then appends its own permission detail, so it is matched by the same check and triaged
  // as a permission setup error.
  def e5 =
    TableFormatSetupError.IcebergRest.check(
      restException(
        403,
        "Not authorized to make this request. Permission 'biglake.catalogs.get' denied on project 'p' (or it may not exist)."
      )
    ) must beSome("Missing privileges on the catalog, schema or table")

  def e6 =
    TableFormatSetupError.IcebergRest.check(
      restException(404, "Project p is not found. Make sure it references valid GCP project that hasn't been deleted.")
    ) must beSome("Unable to find the given GCP project for the catalog")

  // Build the exception through Iceberg's own error handler rather than hand-writing the message, so
  // the test pins the real message format and fails if an Iceberg upgrade changes it. Returns the
  // mapped exception (RESTException, ForbiddenException, ...) for the classifier to inspect.
  private def restException(code: Int, message: String): Throwable =
    try {
      ErrorHandlers.defaultErrorHandler().accept(ErrorResponse.builder().responseCode(code).withMessage(message).build())
      throw new AssertionError(s"expected an exception for code $code")
    } catch {
      case e: RuntimeException => e
    }
}
