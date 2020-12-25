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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.CloseableQueue;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDestinationSqlOperations implements DestinationSqlOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDestinationSqlOperations.class);

  private static final String COLUMN_NAME = "data";

  private final JdbcDatabase database;

  public DefaultDestinationSqlOperations(JdbcDatabase database) {
    this.database = database;
  }

  @Override
  public void createSchema(String schemaName) throws Exception {
    database.execute(createSchemaQuery(schemaName));
  }

  @Override
  public void createDestinationTable(String schemaName, String tableName) throws SQLException {
    database.execute(createDestinationTableQuery(schemaName, tableName));
  }

  @Override
  public void insertBufferedRecords(int batchSize, CloseableQueue<byte[]> writeBuffer, String schemaName, String tmpTableName) throws SQLException {
    final List<AirbyteRecordMessage> records = accumulateRecordsFromBuffer(writeBuffer, batchSize);

    LOGGER.info("max size of batch: {}", batchSize);
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
          "INSERT INTO %s.%s (ab_id, %s, emitted_at) VALUES\n",
          schemaName,
          tmpTableName,
          COLUMN_NAME));

      // first loop: build SQL string.
      records.forEach(r -> sql.append("(?, ?::jsonb, ?),\n"));
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

  @Override
  public String truncateTableQuery(String schemaName, String tableName) {
    return String.format("TRUNCATE TABLE %s.%s;\n", schemaName, tableName);
  }

  @Override
  public String insertIntoFromSelectQuery(String schemaName, String srcTableName, String dstTableName) {
    return String.format("INSERT INTO %s.%s SELECT * FROM %s.%s;\n", schemaName, dstTableName, schemaName, srcTableName);
  }

  /**
   * Accumulate AirbyteRecordMessages from each buffer into batches of records so we can avoid
   * wasteful inserts queries.
   *
   * @param writeBuffer the buffer of messages
   * @param maxRecords up to how many records should be accumulated
   * @return list of messages buffered together in a list
   */
  private List<AirbyteRecordMessage> accumulateRecordsFromBuffer(CloseableQueue<byte[]> writeBuffer, int maxRecords) {
    final List<AirbyteRecordMessage> records = Lists.newArrayList();
    for (int i = 0; i < maxRecords; i++) {
      final byte[] record = writeBuffer.poll();
      if (record == null) {
        break;
      }
      final AirbyteRecordMessage message = Jsons.deserialize(new String(record, Charsets.UTF_8), AirbyteRecordMessage.class);
      records.add(message);
    }

    return records;
  }

  @Override
  public void executeTransaction(String queries) throws Exception {
    database.execute("BEGIN;\n" + queries + "COMMIT;");
  }

  @Override
  public void dropDestinationTable(String schemaName, String tableName) throws SQLException {
    database.execute(dropDestinationTableQuery(schemaName, tableName));
  }

  private String createSchemaQuery(String schemaName) {
    return String.format("CREATE SCHEMA IF NOT EXISTS %s;\n", schemaName);
  }

  @Override
  public String createDestinationTableQuery(String schemaName, String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
            + "ab_id VARCHAR PRIMARY KEY,\n"
            + "%s JSONB,\n"
            + "emitted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP\n"
            + ");\n",
        schemaName, tableName, COLUMN_NAME);
  }

  private String dropDestinationTableQuery(String schemaName, String tableName) {
    return String.format("DROP TABLE IF EXISTS %s.%s;\n", schemaName, tableName);
  }

}
