package database

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doutorfinancas/go-mad/generator"
	mockgenerator "github.com/doutorfinancas/go-mad/mocks/generator"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func getDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)
	return db, mock
}

func getInternalMySQLInstance(db *sql.DB, randomizerService generator.Service) *mySQL {
	dumper, _ := NewMySQLDumper(
		db,
		nil,
		randomizerService,
	)

	return dumper.(*mySQL)
}

func TestMySQLLockTableRead(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectExec("LOCK TABLES `table` READ").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.mysqlLockTableRead("table")
	assert.Nil(t, err)
}

func TestMySQLFlushTable(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectExec("FLUSH TABLES `table`").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.mysqlFlushTable("table")
	assert.Nil(t, err)
}

func TestMySQLUnlockTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.mysqlUnlockTables()
	assert.Nil(t, err)
}

func TestMySQLGetTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).
			AddRow("table1", "BASE TABLE").
			AddRow("table2", "BASE TABLE"),
	)
	tables, err := dumper.getTables()
	assert.Equal(t, []string{"table1", "table2"}, tables)
	assert.Nil(t, err)
}

func TestMySQLGetTablesHandlingErrorWhenListingTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	expectedErr := errors.New("broken")
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnError(expectedErr)
	tables, err := dumper.getTables()
	assert.Equal(t, []string{}, tables)
	assert.Equal(t, expectedErr, err)
}

func TestMySQLGetTablesHandlingErrorWhenScanningRow(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).AddRow(1, nil),
	)
	tables, err := dumper.getTables()
	assert.Equal(t, []string{}, tables)
	assert.NotNil(t, err)
}

func TestMySQLDumpCreateTable(t *testing.T) {
	var ddl = "CREATE TABLE `table` (" +
		"`id` bigint(20) NOT NULL AUTO_INCREMENT, " +
		"`name` varchar(255) NOT NULL, " +
		"PRIMARY KEY (`id`), KEY `idx_name` (`name`) " +
		") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("table", ddl),
	)
	str, err := dumper.getCreateTableStatement("table")

	assert.Nil(t, err)
	assert.Contains(t, str, "DROP TABLE IF EXISTS `table`")
	assert.Contains(t, str, ddl)
}

func TestMySQLDumpCreateTableHandlingErrorWhenScanningRows(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("table", nil),
	)

	_, err := dumper.getCreateTableStatement("table")
	assert.NotNil(t, err)
}

func TestMySQLGetColumnsForSelect(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.selectMap = map[string]map[string]string{"table": {"col2": "NOW()"}}
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("a", "b", "c"),
	)
	columns, err := dumper.getColumnsForSelect("table")
	assert.Nil(t, err)
	assert.Equal(t, []string{"`col1`", "NOW() AS `col2`", "`col3`"}, columns)
}

func TestMySQLGetColumnsForSelectHandlingErrorWhenQuerying(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.selectMap = map[string]map[string]string{"table": {"col2": "NOW()"}}
	err := errors.New("broken")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(err)
	columns, dErr := dumper.getColumnsForSelect("table")
	assert.Equal(t, dErr, err)
	assert.Empty(t, columns)
}

func TestMySQLGetSelectQueryFor(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.selectMap = map[string]map[string]string{"table": {"c2": "NOW()"}}
	dumper.whereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"c1", "c2"}).AddRow("a", "b"),
	)
	query, err := dumper.getSelectQueryFor("table")
	assert.Nil(t, err)
	assert.Equal(t, "SELECT `c1`, NOW() AS `c2` FROM `table` WHERE c1 > 0", query)
}

func TestMySQLGetSelectQueryForHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.selectMap = map[string]map[string]string{"table": {"c2": "NOW()"}}
	dumper.whereMap = map[string]string{"table": "c1 > 0"}
	dErr := errors.New("broken")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(dErr)
	query, err := dumper.getSelectQueryFor("table")
	assert.Equal(t, dErr, err)
	assert.Equal(t, "", query)
}

func TestMySQLGetRowCount(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.whereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234),
	)
	count, err := dumper.rowCount("table")
	assert.Nil(t, err)
	assert.Equal(t, uint64(1234), count)
}

func TestMySQLGetRowCountHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	dumper.whereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil),
	)
	count, err := dumper.rowCount("table")
	assert.NotNil(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestMySQLDumpTableHeader(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234),
	)
	str, count, err := dumper.getTableHeader("table")
	assert.Equal(t, uint64(1234), count)
	assert.Nil(t, err)
	assert.Contains(t, str, "Data for table `table`")
	assert.Contains(t, str, "1234 rows")
}

func TestMySQLDumpTableHeaderHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := getInternalMySQLInstance(db, nil)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil),
	)
	_, count, err := dumper.getTableHeader("table")
	assert.Equal(t, uint64(0), count)
	assert.NotNil(t, err)
}

func TestMySQLDumpTableLockWrite(t *testing.T) {
	dumper := getInternalMySQLInstance(nil, nil)
	str := dumper.getLockTableWriteStatement("table")
	assert.Contains(t, str, "LOCK TABLES `table` WRITE;")
}

func TestMySQLDumpUnlockTables(t *testing.T) {
	dumper := getInternalMySQLInstance(nil, nil)
	str := dumper.getUnlockTablesStatement()
	assert.Contains(t, str, "UNLOCK TABLES;")
}

func TestMySQLDumpTableData(t *testing.T) {
	db, mock := getDB(t)
	buffer := bytes.NewBuffer(make([]byte, 0))

	ctrl := gomock.NewController(t)
	gen := mockgenerator.NewMockService(ctrl)

	dumper := getInternalMySQLInstance(db, gen)
	dumper.extendedInsertLimit = 1

	r := []struct {
		ID    int
		Value string
	}{
		{1, "Lettuce"},
		{2, "Cabbage"},
		{3, "Cucumber"},
		{4, "Potatoes"},
		{5, "Carrot"},
		{6, "Leek"},
	}

	mock.ExpectQuery("SELECT \\* FROM `vegetable_list` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"id", "vegetable"}).
			AddRow(1, "Lettuce"),
	)

	rows := sqlmock.NewRows([]string{"id", "vegetable_list"})
	for _, row := range r {
		rows.AddRow(row.ID, row.Value)
	}
	mock.ExpectQuery("SELECT `id`, `vegetable` FROM `vegetable_list`").
		WillReturnRows(rows)

	assert.Nil(t, dumper.dumpTableData(buffer, "vegetable_list"))

	assert.Equal(t, strings.Count(buffer.String(), "INSERT INTO `vegetable_list` VALUES"), 6)

	for _, row := range r {
		assert.Contains(t, buffer.String(), fmt.Sprintf("'%s'", row.Value))
	}
}

func TestMySQLDumpTableDataHandlingErrorFromSelectAllDataFor(t *testing.T) {
	db, mock := getDB(t)
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := getInternalMySQLInstance(db, nil)
	err := errors.New("fail")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(err)
	assert.Equal(t, err, dumper.dumpTableData(buffer, "table"))
}
