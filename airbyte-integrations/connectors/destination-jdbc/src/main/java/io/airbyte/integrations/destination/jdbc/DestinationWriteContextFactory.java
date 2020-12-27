/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.integrations.destination.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.text.Names;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.WriteConfig;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory class to convert from config and catalog objects into DestinationWriterContext
 * configuration object. This configuration is then used by the RecordConsumers configure their
 * behavior on where to apply their task and data operations
 */
public class DestinationWriteContextFactory {

  private final NamingConventionTransformer namingResolver;

  public DestinationWriteContextFactory(NamingConventionTransformer namingResolver) {
    this.namingResolver = namingResolver;
  }

  public Map<String, WriteConfig> build(JsonNode config, ConfiguredAirbyteCatalog catalog) {
    Preconditions.checkState(config.has("schema"), "jdbc destinations must specify a schema.");
    final Instant now = Instant.now();
    final Map<String, WriteConfig> result = new HashMap<>();
    for (final ConfiguredAirbyteStream stream : catalog.getStreams()) {
      final String streamName = stream.getStream().getName();
      final String schemaName = namingResolver.getIdentifier(config.get("schema").asText());
      final String tableName = Names.concatQuotedNames(namingResolver.getIdentifier(streamName), "_raw");
      final String tmpTableName = Names.concatQuotedNames(tableName, "_" + now.toEpochMilli());
      final SyncMode syncMode = stream.getSyncMode() != null ? stream.getSyncMode() : SyncMode.FULL_REFRESH;
      result.put(streamName, new WriteConfig(streamName, schemaName, tmpTableName, tableName, syncMode));
    }
    return result;
  }

}
