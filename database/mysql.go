package database

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
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
	extendedInsertLimit int
	shouldHexBins       bool
	ignoreGenerated     bool
	dumpTrigger         bool
	skipDefiner         bool
	triggerDelimiter    string
	parallel            bool
}

const (
	ExtendedInsertRows = 100
	IgnoreMapPlacement = "ignore"
	NoDataMapPlacement = "nodata"
	FakerUsageCheck    = "faker"
	MaxConns           = 10
)

var skipDefinerRegExp = regexp.MustCompile(`(?m)DEFINER=[^ ]* `)

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
		shouldHexBins:       false,
		ignoreGenerated:     false,
		dumpTrigger:         false,
		skipDefiner:         false,
		triggerDelimiter:    "",
	}

	err := parseMysqlOptions(m, options)
	if err != nil {
		return nil, err
	}

	if m.parallel {
		db.SetMaxOpenConns(MaxConns)
		db.SetMaxIdleConns(MaxConns)
	} else {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
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
		d.filterMap[strings.ToLower(table)] = NoDataMapPlacement
	}

	for _, table := range d.listTables(t, ignore) {
		d.filterMap[strings.ToLower(table)] = IgnoreMapPlacement
	}

	return nil
}

// Dump creates a MySQL dump and writes it to an io.Writer
// returns error in the event something gos wrong in the middle of the dump process
func (d *mySQL) Dump(w io.Writer) error {
	var dump string
	var tmp string
	var binaryColumns []string
	var generatedColumns []string
	dump = fmt.Sprintf("SET NAMES %s;\n", d.charset)
	dump += "SET FOREIGN_KEY_CHECKS = 0;\n"

	tables, err := d.getTables()
	if err != nil {
		return err
	}

	ctx := context.Background()

	conn, err := newMySQLConn(ctx, d.db, d.singleTransaction)
	if err != nil {
		return err
	}

	defer func(d *mySQL, conn *mySQLConn) {
		err := conn.Close()
		if err != nil {
			d.log.Error(err.Error())
		}
	}(d, conn)

	for _, table := range tables {
		if d.filterMap[strings.ToLower(table)] == IgnoreMapPlacement {
			continue
		}

		skipData := d.filterMap[strings.ToLower(table)] == NoDataMapPlacement
		tmp, err = d.getCreateTableStatement(ctx, conn, table)
		if err != nil {
			return err
		}

		tmp, generatedColumns = d.excludeGeneratedColumns(tmp)

		// this will store if a value we might get is supposed to be hexed cause its binary
		if d.shouldHexBins {
			binaryColumns = d.parseBinaryRelations(tmp)
		}

		dump += tmp
		if !skipData {
			dump, err = d.dumpData(ctx, conn, w, dump, table, generatedColumns, binaryColumns)
			if err != nil {
				return err
			}
		}

		if _, err = fmt.Fprintln(w, dump); err != nil {
			return err
		}

		dump = ""
	}

	if conn.singleTransaction {
		err = conn.Commit()
		if err != nil {
			// we actually don't require this commit to be performed
			// just making sure everything is fine with the transaction
			// and no dangling pieces are left. Should log though
			d.log.Error("could not commit transaction")
		}
	}

	_, err = fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")

	if d.dumpTrigger {
		if err := d.dumpTriggers(ctx, conn, w); err != nil {
			return err
		}
	}

	return err
}

func (d *mySQL) parseBinaryRelations(createTable string) []string {
	binaryColumns := make([]string, 0)

	scanner := bufio.NewScanner(strings.NewReader(createTable))
	for scanner.Scan() {
		if strings.Contains(strings.ToLower(scanner.Text()), "binary") {
			r := regexp.MustCompile("`([^(]*)`")
			columnName := r.FindAllStringSubmatch(scanner.Text(), -1)

			if len(columnName) > 0 && len(columnName[0]) > 0 {
				binaryColumns = append(binaryColumns, columnName[0][1])
			}
		}
	}

	return binaryColumns
}

func (d *mySQL) excludeGeneratedColumns(createTable string) (string, []string) {
	generatedColumns := make([]string, 0)
	tmp := ""

	scanner := bufio.NewScanner(strings.NewReader(createTable))
	for scanner.Scan() {
		if !strings.Contains(strings.ToLower(scanner.Text()), "generated always") {
			tmp += scanner.Text() + "\n"
		} else {
			r := regexp.MustCompile("`([^(]*)`")
			columnName := r.FindAllStringSubmatch(scanner.Text(), -1)

			if len(columnName) > 0 && len(columnName[0]) > 0 {
				generatedColumns = append(generatedColumns, columnName[0][1])
			}
		}
	}

	if !d.ignoreGenerated {
		return createTable, generatedColumns
	}

	if createTable[len(createTable)-1:] != "\n" {
		return tmp[:len(tmp)-1], generatedColumns
	}

	return tmp, generatedColumns
}

func (d *mySQL) dumpData(ctx context.Context, conn *mySQLConn, w io.Writer, dump, table string, generatedColumns, binaryColumns []string) (string, error) {
	var cnt uint64
	var tmp string
	var err error
	if d.lockTables {
		_, err = d.mysqlFlushTable(ctx, conn, table)
		if err != nil {
			return "", err
		}
	}

	tmp, cnt, err = d.getTableHeader(ctx, conn, table)
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

		if dErr := d.dumpTableData(ctx, conn, w, table, generatedColumns, binaryColumns); dErr != nil {
			return "", dErr
		}

		if d.addLocks {
			dump += d.getUnlockTablesStatement()
		}
	}

	if d.lockTables {
		if _, dErr := d.mysqlUnlockTables(ctx, conn); dErr != nil {
			return "", dErr
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

func (d *mySQL) dumpTableData(ctx context.Context, conn *mySQLConn, w io.Writer, table string, generatedColumns, binaryColumns []string) error {
	columns, err := d.getColumnsForSelect(ctx, conn, table, false, generatedColumns)

	if err != nil {
		return err
	}

	rows, _, err := d.selectAllDataFor(ctx, conn, table, generatedColumns)
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

	query := d.generateInsertStatement(columns, table)
	var data []string
	for rows.Next() {
		if dErr := rows.Scan(scanArgs...); err != nil {
			return dErr
		}
		var vals []string
		for i, col := range values {
			vals = append(vals, d.getProperEscapedValue(col, columns[i], binaryColumns))
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

func (d *mySQL) getProperEscapedValue(col *sql.RawBytes, columnName string, binaryColumns []string) string {
	val := "NULL"

	if col != nil {
		if d.shouldHexBins && core.InSlice(binaryColumns, strings.Trim(columnName, "`")) {
			val = fmt.Sprintf("UNHEX('%s')", hex.EncodeToString(*col))
		} else {
			val = string(*col)

			if len(val) >= 5 && val[0:5] == FakerUsageCheck {
				val, _ = d.randomizerService.ReplaceStringWithFakerWhenRequested(val)
			}

			val = fmt.Sprintf("'%s'", escape(val))
		}
	}

	return val
}

func (d *mySQL) generateInsertStatement(cols []string, table string) string {
	s := fmt.Sprintf("INSERT INTO `%s` (", table)
	for _, col := range cols {
		s += fmt.Sprintf("%s, ", col)
	}

	return s[:len(s)-2] + ") VALUES"
}

func (d *mySQL) getTableHeader(ctx context.Context, conn *mySQLConn, table string) (str string, count uint64, err error) {
	str = fmt.Sprintf("\n--\n-- Data for table `%s`", table)
	count, err = d.rowCount(ctx, conn, table)

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

func (d *mySQL) selectAllDataFor(ctx context.Context, conn *mySQLConn, table string, generatedColumns []string) (rows *sql.Rows, columns []string, err error) {
	var selectQuery string
	if columns, selectQuery, err = d.getSelectQueryFor(ctx, conn, table, generatedColumns); err != nil {
		return
	}
	if rows, err = conn.conn.QueryContext(ctx, selectQuery); err != nil {
		return
	}

	return
}

func (d *mySQL) getSelectQueryFor(ctx context.Context, conn *mySQLConn, table string, generatedColumns []string) (cols []string, query string, err error) {
	cols, err = d.getColumnsForSelect(ctx, conn, table, true, generatedColumns)
	if err != nil {
		return cols, "", err
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

func (d *mySQL) getColumnsForSelect(ctx context.Context, conn *mySQLConn, table string, considerRewriteMap bool, generatedColumns []string) (columns []string, err error) {
	rows, err := conn.conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table))
	if a := d.evaluateErrors(err, rows); a != nil {
		return columns, a
	}

	defer func(rows *sql.Rows) {
		dErr := rows.Close()
		if dErr != nil {
			d.log.Warn(dErr.Error(), zap.String("table", table))
		}
	}(rows)
	var tmp []string
	if tmp, err = rows.Columns(); err != nil {
		return
	}

	for _, column := range tmp {
		if core.InSlice(generatedColumns, column) {
			continue
		}

		replacement, ok := d.selectMap[strings.ToLower(table)][strings.ToLower(column)]
		if ok && considerRewriteMap {
			if len(replacement) >= 5 && replacement[0:5] == FakerUsageCheck {
				replacement = fmt.Sprintf("'%s'", replacement)
			}

			columns = append(columns, fmt.Sprintf("%s AS `%s`", replacement, column))
		} else {
			columns = append(columns, fmt.Sprintf("`%s`", column))
		}
	}

	return columns, nil
}

func (d *mySQL) rowCount(ctx context.Context, conn *mySQLConn, table string) (uint64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if where, ok := d.whereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}

	var count uint64
	row, err := conn.useTransactionOrDBQueryRow(ctx, query)
	if err != nil {
		return 0, err
	}

	if err = row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (d *mySQL) getCreateTableStatement(ctx context.Context, conn *mySQLConn, table string) (string, error) {
	s := fmt.Sprintf("\n--\n-- Structure for table `%s`\n--\n\n", table)
	s += fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table)
	row, err := conn.useTransactionOrDBQueryRow(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	if err != nil {
		return "", err
	}

	var tname, ddl string
	if err = row.Scan(&tname, &ddl); err != nil {
		return "", err
	}
	s += fmt.Sprintf("%s;\n", ddl)
	return s, nil
}

func (d *mySQL) mysqlFlushTable(ctx context.Context, conn *mySQLConn, table string) (sql.Result, error) {
	return conn.useTransactionOrDBExec(ctx, fmt.Sprintf("FLUSH TABLES `%s` WITH READ LOCK", table))
}

// Release the global read locks
func (d *mySQL) mysqlUnlockTables(ctx context.Context, conn *mySQLConn) (sql.Result, error) {
	return conn.useTransactionOrDBExec(ctx, "UNLOCK TABLES")
}

func (d *mySQL) dumpTriggers(ctx context.Context, conn *mySQLConn, w io.Writer) error {
	triggers, err := d.getTriggers(ctx, conn)
	if err != nil {
		return err
	}

	for _, trigger := range triggers {
		ddl, err := d.getTrigger(ctx, conn, trigger)

		if err != nil {
			return err
		}

		fmt.Fprintf(w, "\n--\n-- Trigger `%s`\n--\n\n", trigger)

		if d.triggerDelimiter != "" {
			fmt.Fprintf(w, "DELIMITER %s\n", d.triggerDelimiter)
		}

		if _, err := w.Write([]byte(ddl)); err != nil {
			return err
		}

		if d.triggerDelimiter != "" {
			fmt.Fprintf(w, "%s\nDELIMITER ;\n", d.triggerDelimiter)
		}
	}

	return nil
}

func (d *mySQL) getTriggers(ctx context.Context, conn *mySQLConn) ([]string, error) {
	triggers := make([]string, 0)

	rows, err := conn.conn.QueryContext(ctx, "SHOW TRIGGERS")
	if a := d.evaluateErrors(err, rows); a != nil {
		return triggers, a
	}

	defer func(rows *sql.Rows) {
		dErr := rows.Close()
		if dErr != nil {
			d.log.Error(
				dErr.Error(),
				zap.String("internal", "failed to close rows while getting triggers"),
			)
		}
	}(rows)

	for rows.Next() {
		var triggerName, unknown string

		if dErr := rows.Scan(&triggerName, &unknown, &unknown, &unknown, &unknown, &unknown, &unknown, &unknown, &unknown, &unknown, &unknown); dErr != nil {
			return triggers, dErr
		}

		triggers = append(triggers, triggerName)
	}

	return triggers, nil
}

func (d *mySQL) getTrigger(ctx context.Context, conn *mySQLConn, triggerName string) (string, error) {
	var ddl, unknown string

	row, err := conn.useTransactionOrDBQueryRow(ctx, fmt.Sprintf("SHOW CREATE TRIGGER `%s`", triggerName))
	if err != nil {
		return "", err
	}

	if err := row.Scan(&unknown, &unknown, &ddl, &unknown, &unknown, &unknown, &unknown); err != nil {
		return "", err
	}

	if d.skipDefiner {
		ddl = skipDefinerRegExp.ReplaceAllString(ddl, "")
	}

	return ddl + ";\n", nil
}
