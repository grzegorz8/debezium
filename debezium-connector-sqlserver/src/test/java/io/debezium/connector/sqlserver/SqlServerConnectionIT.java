/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;
import java.math.BigInteger;
import java.sql.SQLException;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for {@link SqlServerConnection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnectionIT {

    @Before
    public void before() throws SQLException {
        TestHelper.dropTestDatabase();
    }

    @Test
    public void shouldEnableCdcForDatabase() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");
        }
    }

    @Test
    public void shouldEnableCdcWithWrapperFunctionsForTable() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'testTable' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testTable (ID int not null identity(1, 1) primary key, NUMBER int, TEXT text)";
            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "testTable");
            // insert some data

            connection.execute("INSERT INTO testTable (NUMBER, TEXT) values (1, 'aaa')\n"
                    + "INSERT INTO testTable (NUMBER, TEXT) values (2, 'bbb')");

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available

            Testing.Print.enable();
            connection.query(
                    "select * from cdc.fn_cdc_get_all_changes_dbo_testTable(sys.fn_cdc_get_min_lsn('dbo_testTable'), sys.fn_cdc_get_max_lsn(), N'all')",
                    rs -> {
                        while (rs.next()) {
                            final BigInteger lsn = new BigInteger(rs.getBytes(1));
                            final StringBuilder sb = new StringBuilder(lsn.toString());
                            for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                                sb.append(rs.getObject(col)).append(' ');
                            }
                            Testing.print(sb.toString());
                        }
                    });
            Testing.Print.disable();
        }

    }

    @Test
    public void shouldProperlyGetDefaultColumnValues() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'testTable' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testTable ("
                    + "  ID int not null identity(1, 1) primary key,"
                    + "  NUMBER int DEFAULT(123),"
                    + "  TEXT text DEFAULT('dummy')"
                    + ")";
            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "testTable");
            // insert some data

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available

            Testing.Print.enable();

            ChangeTable changeTable = new ChangeTable(new TableId("testDB", "dbo", "testTable"),
                    null, 0, null, null);
            Table table = connection.getTableSchemaFromTable(changeTable);


            assertColumnHasDefaultValue(table, "NUMBER", 123);
            assertColumnHasDefaultValue(table, "TEXT", "dummy");

            Testing.Print.disable();
        }
    }

    @Test
    public void shouldProperlyGetDefaultColumnValues2() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");

            // create table if exists
            String sql = "CREATE TABLE testDB.dbo.testowa (" +
//                    "    bigint_column bigint default (3147483648)," +
//                    "    int_column int default (2147483647)," +
//                    "    smallint_column smallint default (32767)," +
//                    "    tinyint_column tinyint default (255)," +
//                    "    bit_column bit default(1)," +
//                    "    decimal_column decimal(20,5) default (100.12345)," +
//                    "    numeric_column numeric(10,3) default (200.123)," +
//                    "    money_column money default (922337203685477.58)," +
//                    "    smallmoney_column smallmoney default (214748.3647)," +
//                      "  float_column float default (1.2345e3)," +
//                      "  real_column real default (1.2345e3)" +
//                    "  date_column date default ('2019-02-03')," +
//                    "  datetime_column datetime default ('2019-01-01 00:00:00.000')," +
//                    "  datetime2_column datetime2 default ('2019-01-01 00:00:00.1234567')," +
//                    "  datetimeoffset_column datetimeoffset default ('2019-01-01 00:00:00.1234567+02:00')," +
//                    "  smalldatetime_column smalldatetime default ('2019-01-01 00:00:00')," +
//                    "  time_column time default ('12:34:56')" +
//                    "  char_column char(10) default ('a')," +
//                    "  varchar_column varchar(20) default ('a')," +
//                    "  text_column text default ('a')," +
//                    "  nchar_column nchar(11) default ('a')," +
//                    "  nvarchar_column nvarchar(20) default ('a')," +
//                    "  ntext_column ntext default ('a')," +
                    "  binary_column binary(5) default (0x0102030405)," +
                    "  varbinary_column varbinary(10) default (0x010203040506)," +
                    "  image_column image default (0x010203040507)" +
                    ");";

            connection.execute(sql);

//            sql = "INSERT INTO testDB.dbo.testowa values (3147483647, 2147483646, 32766, 254, 0, 100.54321, 200.321, 922337203685477.57, 214748.3646, 5.4321e3, 5.4321e2);";
            sql = "INSERT INTO testDB.dbo.testowa values ("
//                    + "'2019-01-01',"
//                    + "'2019-01-01 00:00:00.000',"
//                    + "'2019-01-01 00:00:00.1234567',"
//                    + "'2019-01-01 00:00:00.1234567+02:00',"
//                    + "'2019-01-01 00:00:00',"
//                    + "'12:34:56'"

//                      + "'char_value',"
//                      + "'varchar_value',"
//                      + "'text_value',"
//                      + "'nchar_value',"
//                      + "'nvarchar_value',"
//                      + "'ntext_value',"
                      + "0x0102030405,"
                      + "0x010203040506,"
                      + "0x01020304050607"
                    + ");";
            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "testTable");
            // insert some data

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available

            Testing.Print.enable();

            connection.getData();
            ChangeTable changeTable = new ChangeTable(new TableId("testDB", "dbo", "testowa"),
                    null, 0, null, null);
            Table table = connection.getTableSchemaFromTable(changeTable);


//            assertColumnHasDefaultValue(table, "NUMBER", 123);
//            assertColumnHasDefaultValue(table, "TEXT", "dummy");

            Testing.Print.disable();
        }
    }

    private void assertColumnHasDefaultValue(Table table, String columnName, Object expectedValue) {
        Column column = table.columnWithName(columnName);
        Assertions.assertThat(column.hasDefaultValue()).isTrue();
        Assertions.assertThat(column.defaultValue()).isEqualTo(expectedValue);
    }

}
