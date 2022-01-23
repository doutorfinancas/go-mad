package database

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/doutorfinancas/go-mad/core"
	"github.com/doutorfinancas/go-mad/generator"
	_ "github.com/go-sql-driver/mysql" // adds mysql database
	"github.com/gobwas/glob"
	"go.uber.org/zap"
)

type MySQL interface {
	Dump(w io.Writer) (err error)
	SetSelectMap(map[string]map[string]string)
	SetWhereMap(map[string]string)
	SetFilterMap(noData []string, ignore []string) error
}

type mySQL struct {
	db                  *sql.DB
	log                 *zap.Logger
	selectMap           map[string]map[string]string
	whereMap            map[string]string
	filterMap           map[string]string
	lockTables          bool
	charset             string
	quick               bool
	singleTransaction   bool
	addLocks            bool
	randomizerService   generator.Service
	openTx              *sql.Tx
	extendedInsertLimit int
}

const (
	ExtendedInsertRows = 100
	IgnoreMapPlacement = "ignore"
	NoDataMapPlacement = "nodata"
)

func NewMySQLDumper(db *sql.DB, logger *zap.Logger, randomizerService generator.Service, options ...Option) (
	MySQL,
	error,
) {
	m := &mySQL{
		db:                  db,
		log:                 logger,
		quick:               false,
		charset:             "utf8",
		singleTransaction:   false,
		lockTables:          true,
		addLocks:            true,
		extendedInsertLimit: ExtendedInsertRows,
		randomizerService:   randomizerService,
	}

	err := parseMysqlOptions(m, options)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (d *mySQL) SetSelectMap(m map[string]map[string]string) {
	d.selectMap = m
}

func (d *mySQL) SetWhereMap(m map[string]string) {
	d.whereMap = m
}

func (d *mySQL) SetFilterMap(noData, ignore []string) error {
	d.filterMap = make(map[string]string)

	t, err := d.getTables()
	if err != nil {
		return err
	}
	for _, table := range d.listTables(t, noData) {
		d.filterMap[table] = NoDataMapPlacement
	}

	for _, table := range d.listTables(t, ignore) {
		d.filterMap[table] = IgnoreMapPlacement
	}

	return nil
}

// Dump creates a MySQL dump and writes it to an io.Writer
// returns error in the event something gos wrong in the middle of the dump process
func (d *mySQL) Dump(w io.Writer) error {
	var dump string
	var tmp string
	dump = fmt.Sprintf("SET NAMES %s;\n", d.charset)
	dump += "SET FOREIGN_KEY_CHECKS = 0;\n"

	tables, err := d.getTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if d.filterMap[strings.ToLower(table)] == IgnoreMapPlacement {
			continue
		}

		skipData := d.filterMap[strings.ToLower(table)] == NoDataMapPlacement
		if !skipData && d.lockTables {
			_, err = d.mysqlLockTableRead(table)
			if err != nil {
				return err
			}
			_, err = d.mysqlFlushTable(table)
			if err != nil {
				return err
			}
		}

		tmp, err = d.getCreateTableStatement(table)
		if err != nil {
			return err
		}

		dump += tmp
		if !skipData {
			dump, err = d.dumpData(w, dump, table)
			if err != nil {
				return err
			}
		}

		if _, err = fmt.Fprintln(w, dump); err != nil {
			d.log.Error(err.Error())
		}
	}

	if d.singleTransaction {
		err = d.openTx.Commit()
		if err != nil {
			// we actually don't require this commit to be performed
			// just making sure everything is fine with the transaction
			// and no dangling pieces are left. Should log though
			d.log.Error("could not commit transaction")
		}
	}

	_, err = fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")
	return err
}

func (d *mySQL) dumpData(w io.Writer, dump, table string) (string, error) {
	var cnt uint64
	var tmp string
	var err error
	tmp, cnt, err = d.getTableHeader(table)
	if err != nil {
		return "", err
	}
	dump += tmp
	if cnt > 0 {
		if d.addLocks {
			dump += d.getLockTableWriteStatement(table)
		}

		// before the data dump, we need to flush everything to file
		if _, err = fmt.Fprintln(w, dump); err != nil {
			return "", err
		}
		// and after flush we need to clear the variable
		dump = ""

		if dErr := d.dumpTableData(w, table); dErr != nil {
			return "", dErr
		}

		if d.addLocks {
			dump += d.getUnlockTablesStatement()
		}

		if d.lockTables {
			if _, dErr := d.mysqlUnlockTables(); err != nil {
				return "", dErr
			}
		}
	}

	return dump, nil
}

func (d *mySQL) listTables(tables, globs []string) []string {
	var globbed []string

	for _, query := range globs {
		g := glob.MustCompile(query)

		for _, table := range tables {
			if g.Match(table) {
				globbed = core.AppendIfNotExists(globbed, table)
			}
		}
	}

	return globbed
}

func (d *mySQL) getTables() ([]string, error) {
	tables := make([]string, 0)

	rows, err := d.db.Query("SHOW FULL TABLES")
	if a := d.evaluateErrors(err, rows); a != nil {
		return tables, a
	}

	defer func(rows *sql.Rows) {
		dErr := rows.Close()
		if dErr != nil {
			d.log.Error(
				dErr.Error(),
				zap.String("internal", "failed to close rows while getting tables"),
			)
		}
	}(rows)

	for rows.Next() {
		var tableName, tableType string

		if dErr := rows.Scan(&tableName, &tableType); dErr != nil {
			return tables, dErr
		}

		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}

	return tables, nil
}

func (d *mySQL) dumpTableData(w io.Writer, table string) error {
	rows, columns, err := d.selectAllDataFor(table)
	if a := d.evaluateErrors(err, rows); a != nil {
		return a
	}

	defer func(rows *sql.Rows) {
		dErr := rows.Close()
		if dErr != nil {
			d.log.Error(
				dErr.Error(),
				zap.String("table", table),
				zap.String("context", "dumping data, closing rows failed"),
			)
		}
	}(rows)

	numRows := d.extendedInsertLimit
	if d.quick {
		numRows = 1
	}

	values := make([]*sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	query := fmt.Sprintf("INSERT INTO `%s` VALUES", table)
	var data []string
	for rows.Next() {
		if dErr := rows.Scan(scanArgs...); err != nil {
			return dErr
		}
		var vals []string
		for _, col := range values {
			val := "NULL"
			if col != nil {
				val = fmt.Sprintf("'%s'", escape(string(*col)))
			}

			val, _ = d.randomizerService.ReplaceStringWithFakerWhenRequested(val)
			vals = append(vals, val)
		}

		data = append(data, fmt.Sprintf("( %s )", strings.Join(vals, ", ")))
		if len(data) >= numRows {
			fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
			data = make([]string, 0)
		}
	}

	if len(data) > 0 {
		fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
	}

	return nil
}

func (d *mySQL) getTableHeader(table string) (str string, count uint64, err error) {
	str = fmt.Sprintf("\n--\n-- Data for table `%s`", table)
	count, err = d.rowCount(table)

	if err != nil {
		return "", 0, err
	}

	str += fmt.Sprintf(" -- %d rows\n--\n\n", count)
	return
}

func (d *mySQL) evaluateErrors(base error, rows *sql.Rows) error {
	if base != nil {
		return base
	}

	if rows != nil && rows.Err() != nil {
		return rows.Err()
	}

	return nil
}

func (d *mySQL) selectAllDataFor(table string) (rows *sql.Rows, columns []string, err error) {
	var selectQuery string
	if selectQuery, err = d.getSelectQueryFor(table); err != nil {
		return
	}
	if rows, err = d.db.Query(selectQuery); err != nil {
		return
	}
	if columns, err = rows.Columns(); err != nil {
		return
	}
	return
}

func (d *mySQL) getSelectQueryFor(table string) (query string, err error) {
	cols, err := d.getColumnsForSelect(table)
	if err != nil {
		return "", err
	}
	query = fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(cols, ", "), table)
	if where, ok := d.whereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

func (d *mySQL) getLockTableWriteStatement(table string) string {
	return fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", table)
}

func (d *mySQL) getUnlockTablesStatement() string {
	return "UNLOCK TABLES;\n"
}

func (d *mySQL) getColumnsForSelect(table string) (columns []string, err error) {
	rows, err := d.db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table))
	if a := d.evaluateErrors(err, rows); a != nil {
		return columns, a
	}

	defer func(rows *sql.Rows) {
		dErr := rows.Close()
		if dErr != nil {
			d.log.Warn(
				dErr.Error(),
				zap.String("table", table),
			)
		}
	}(rows)
	if columns, err = rows.Columns(); err != nil {
		return
	}
	for k, column := range columns {
		replacement, ok := d.selectMap[strings.ToLower(table)][strings.ToLower(column)]
		if ok {
			columns[k] = fmt.Sprintf("%s AS `%s`", replacement, column)
		} else {
			columns[k] = fmt.Sprintf("`%s`", column)
		}
	}
	return
}

func (d *mySQL) rowCount(table string) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if where, ok := d.whereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	row := d.useTransactionOrDBQueryRow(query)
	if err = row.Scan(&count); err != nil {
		return
	}
	return
}

func (d *mySQL) getCreateTableStatement(table string) (string, error) {
	s := fmt.Sprintf("\n--\n-- Structure for table `%s`\n--\n\n", table)
	s += fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table)
	row := d.useTransactionOrDBQueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tname, ddl string
	if err := row.Scan(&tname, &ddl); err != nil {
		return "", err
	}
	s += fmt.Sprintf("%s;\n", ddl)
	return s, nil
}

func (d *mySQL) mysqlLockTableRead(table string) (sql.Result, error) {
	return d.useTransactionOrDBExec(fmt.Sprintf("LOCK TABLES `%s` READ", table))
}
func (d *mySQL) mysqlFlushTable(table string) (sql.Result, error) {
	return d.useTransactionOrDBExec(fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// Release the global read locks
func (d *mySQL) mysqlUnlockTables() (sql.Result, error) {
	return d.useTransactionOrDBExec("UNLOCK TABLES")
}

func (d *mySQL) useTransactionOrDBQueryRow(query string) *sql.Row {
	if d.singleTransaction {
		return d.getTransaction().QueryRow(query)
	}

	return d.db.QueryRow(query)
}

func (d *mySQL) useTransactionOrDBExec(query string) (sql.Result, error) {
	if d.singleTransaction {
		return d.getTransaction().Exec(query)
	}

	return d.db.Exec(query)
}

func (d *mySQL) getTransaction() *sql.Tx {
	if d.openTx == nil {
		var err error
		d.openTx, err = d.db.Begin()
		if err != nil {
			panic("could not start a transaction")
		}
	}

	return d.openTx
}
