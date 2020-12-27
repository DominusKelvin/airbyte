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
import io.airbyte.commons.functional.CheckedBiConsumer;
import io.airbyte.integrations.base.DestinationConsumer;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This factory builds pipelines of DestinationConsumer of AirbyteMessage with different strategies.
 */
public class SqlDestinationConsumerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDestinationConsumerFactory.class);

  /**
   * This pipeline is based on DestinationConsumers that can interact with some kind of Sql based
   * database destinations.
   *
   * The Strategy used here is:
   * <p>
   * 1. Create a temporary table for each stream
   * </p>
   * <p>
   * 2. Accumulate records in a buffer. One buffer per stream.
   * </p>
   * <p>
   * 3. As records accumulate write them in batch to the database. We set a minimum numbers of records
   * before writing to avoid wasteful record-wise writes.
   * </p>
   * <p>
   * 4. Once all records have been written to buffer, flush the buffer and write any remaining records
   * to the database (regardless of how few are left).
   * </p>
   * <p>
   * 5. In a single transaction, delete the target tables if they exist and rename the temp tables to
   * the final table name.
   * </p>
   *
   * @param sqlOperations is a destination based on Sql engine which provides certain SQL Queries to
   *        interact with
   * @param namingResolver is a SQLNamingResolvable object to translate strings into valid identifiers
   *        supported by the underlying Sql Database
   * @param config the destination configuration object
   * @param catalog describing the streams of messages to write to the destination
   * @return A DestinationConsumer able to accept the Airbyte Messages
   * @throws Exception
   */
  public static DestinationConsumer<AirbyteMessage> build(DestinationSqlOperations sqlOperations,
                                                          NamingConventionTransformer namingResolver,
                                                          JsonNode config,
                                                          ConfiguredAirbyteCatalog catalog)
      throws Exception {
    final Map<String, DestinationWriteContext> writeConfigs =
        new DestinationWriteContextFactory(namingResolver).build(config, catalog);
    // Step 2, 3 & 4
    return new BufferedStreamConsumer2(
        blah(sqlOperations),
        blah2(sqlOperations),
        sqlOperations,
        catalog,
        writeConfigs);
  }

  private static CheckedBiConsumer<DestinationWriteContext, Stream<AirbyteRecordMessage>, Exception> blah(
                                                                                                          DestinationSqlOperations sqlOperations) {
    return (writeContext, recordStream) -> {
      sqlOperations.insertBufferedRecords(recordStream, writeContext.getOutputNamespaceName(), writeContext.getTmpTableName());
    };
  }

  private static CheckedBiConsumer<Boolean, List<DestinationWriteContext>, Exception> blah2(
                                                                                            DestinationSqlOperations sqlOperations) {
    return (hasFailed, writeContexts) -> {
      // copy data
      if (!hasFailed) {
        final StringBuilder queries = new StringBuilder();
        for (DestinationWriteContext writeContext : writeContexts) {
          final String schemaName = writeContext.getOutputNamespaceName();
          final String srcTableName = writeContext.getTmpTableName();
          final String dstTableName = writeContext.getOutputTableName();

          sqlOperations.createDestinationTable(schemaName, dstTableName);
          switch (writeContext.getSyncMode()) {
            case FULL_REFRESH -> queries.append(sqlOperations.truncateTableQuery(schemaName, dstTableName));
            case INCREMENTAL -> {}
            default -> throw new IllegalStateException("Unrecognized sync mode: " + writeContext.getSyncMode());
          }
          queries.append(sqlOperations.insertIntoFromSelectQuery(schemaName, srcTableName, dstTableName));
          try {
            sqlOperations.executeTransaction(queries.toString());
          } catch (Exception e) {
            LOGGER.error(String.format("Failed to write %s.%s because of ", schemaName, dstTableName), e);
          }
        }
        sqlOperations.executeTransaction(queries.toString());
      }
      // clean up
      for (DestinationWriteContext writeContext : writeContexts) {
        final String schemaName = writeContext.getOutputNamespaceName();
        final String tmpTableName = writeContext.getOutputTableName();
        sqlOperations.dropDestinationTable(schemaName, tmpTableName);
      }
    };
  }

}
