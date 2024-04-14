/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

/**
 * Hoodie Configs for Glue.
 */
@ConfigClassProperty(name = "Glue catalog sync based client Configurations",
    groupName = ConfigGroups.Names.META_SYNC,
    subGroupName = ConfigGroups.SubGroupNames.NONE,
    description = "Configs that control Glue catalog sync based client.")
public class GlueCatalogSyncClientConfig extends HoodieConfig {
  public static final String GLUE_CLIENT_PROPERTY_PREFIX = "hoodie.datasource.meta.sync.glue.";

  public static final ConfigProperty<Boolean> GLUE_SKIP_TABLE_ARCHIVE = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "skip_table_archive")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Glue catalog sync based client will skip archiving the table version if this config is set to true");

  public static final ConfigProperty<Boolean> GLUE_METADATA_FILE_LISTING = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "metadata_file_listing")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Makes athena use the metadata table to list partitions and files. Currently it won't benefit from other features such stats indexes");

  /* Start of section added by Snowplow */

  public static final ConfigProperty<String> GLUE_REGION = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "region")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("snowplow")
      .withDocumentation("AWS region of the Glue Catalog");

  public static final ConfigProperty<String> GLUE_ASSUMED_ROLE_ARN = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "assume.role.arn")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("snowplow")
      .withDocumentation("ARN of a role to assume when syncing with Glue, e.g. arn:aws:iam::123456789:role/myRoleToAssume");

  public static final ConfigProperty<String> GLUE_ASSUMED_ROLE_SESSION_NAME = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "assume.role.session.name")
      .defaultValue("hudi-aws")
      .markAdvanced()
      .sinceVersion("snowplow")
      .withDocumentation("Session name to use if assuming a role when syncing with Glue");

  public static final ConfigProperty<String> GLUE_ASSUMED_ROLE_SESSION_EXTERNAL_ID = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "assume.role.session.external.id")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("snowplow")
      .withDocumentation("An optional external ID to use when assuming a AWS role");

  public static final ConfigProperty<Integer> GLUE_ASSUMED_ROLE_SESSION_DURATION_SECONDS = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "assume.role.session.duration.seconds")
      .defaultValue(3600)
      .markAdvanced()
      .sinceVersion("snowplow")
      .withDocumentation("Duration of session credentials when assuming a AWS role");

  /* End of section added by Snowplow */
}
