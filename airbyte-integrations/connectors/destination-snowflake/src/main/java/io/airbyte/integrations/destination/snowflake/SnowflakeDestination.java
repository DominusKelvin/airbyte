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

package io.airbyte.integrations.destination.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.DestinationConsumer;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.jdbc.DefaultDestinationSqlOperations;
import io.airbyte.integrations.destination.jdbc.DestinationSqlOperations;
import io.airbyte.integrations.destination.jdbc.SqlDestinationConsumerFactory;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeDestination implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDestination.class);

  protected static final String COLUMN_NAME = "data";

  private final SnowflakeSQLNameTransformer namingResolver;

  public SnowflakeDestination() {
    namingResolver = new SnowflakeSQLNameTransformer();
  }

  @Override
  public ConnectorSpecification spec() throws IOException {
    final String resourceString = MoreResources.readResource("spec.json");
    return Jsons.deserialize(resourceString, ConnectorSpecification.class);
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      final JdbcDatabase database = SnowflakeDatabase.getDatabase(config);
      database.execute("SELECT 1;");
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (Exception e) {
      return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(e.getMessage());
    }
  }

  @Override
  public NamingConventionTransformer getNamingTransformer() {
    return namingResolver;
  }

  @Override
  public DestinationConsumer<AirbyteMessage> write(JsonNode config, ConfiguredAirbyteCatalog catalog) throws Exception {
    final DestinationSqlImpl destination = new DestinationSqlImpl(SnowflakeDatabase.getDatabase(config));
    return SqlDestinationConsumerFactory.build(destination, getNamingTransformer(), config, catalog);
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new SnowflakeDestination();
    LOGGER.info("starting destination: {}", SnowflakeDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", SnowflakeDestination.class);
  }

  private static class DestinationSqlImpl extends DefaultDestinationSqlOperations implements DestinationSqlOperations {

    private final JdbcDatabase database;

    public DestinationSqlImpl(JdbcDatabase database) {
      super(database);
      this.database = database;
    }

    @Override
    public void createDestinationTable(String schemaName, String tableName) throws SQLException {
      final String createTableQuery = String.format(
          "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
              + "ab_id VARCHAR PRIMARY KEY,\n"
              + "\"%s\" VARIANT,\n"
              + "emitted_at TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp()\n"
              + ") data_retention_time_in_days = 0;",
          schemaName, tableName, COLUMN_NAME);
      database.execute(createTableQuery);
    }

    @Override
    public void insertBufferedRecords(Stream<AirbyteRecordMessage> recordsStream, String schemaName, String tmpTableName) throws SQLException {
      final List<AirbyteRecordMessage> records = recordsStream.collect(Collectors.toList());

      LOGGER.info("actual size of batch: {}", records.size());

      if (records.isEmpty()) {
        return;
      }

      database.execute(connection -> {

        // Strategy: We want to use PreparedStatement because it handles binding values to the SQL query
        // (e.g. handling formatting timestamps). A PreparedStatement statement is created by supplying the
        // full SQL string at creation time. Then subsequently specifying which values are bound to the
        // string. Thus there will be two loops below.
        // 1) Loop over records to build the full string.
        // 2) Loop over the records and bind the appropriate values to the string.
        final StringBuilder sql = new StringBuilder(String.format(
            "INSERT INTO %s.%s (ab_id, \"%s\", emitted_at) SELECT column1, parse_json(column2), column3 FROM VALUES\n",
            schemaName,
            tmpTableName,
            COLUMN_NAME));

        // first loop: build SQL string.
        records.forEach(r -> sql.append("(?, ?, ?),\n"));
        final String s = sql.toString();
        final String s1 = s.substring(0, s.length() - 2) + ";";

        try (final PreparedStatement statement = connection.prepareStatement(s1)) {
          // second loop: bind values to the SQL string.
          int i = 1;
          for (final AirbyteRecordMessage message : records) {
            // 1-indexed
            statement.setString(i, UUID.randomUUID().toString());
            statement.setString(i + 1, Jsons.serialize(message.getData()));
            statement.setTimestamp(i + 2, Timestamp.from(Instant.ofEpochMilli(message.getEmittedAt())));
            i += 3;
          }

          statement.execute();
        }
      });
    }

  }

}
