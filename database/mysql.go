package database

import (
	"database/sql"
	"fmt"
	"github.com/doutorfinancas/go-mad/core"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gobwas/glob"
	"go.uber.org/zap"
	"io"
	"strings"
)

type MySql interface {
	Dump(w io.Writer) (err error)
	SetSelectMap(map[string]map[string]string)
	SetWhereMap(map[string]string)
	SetFilterMap(noData []string, ignore []string) error
}

type mySql struct {
	db                *sql.DB
	log               *zap.Logger
	selectMap         map[string]map[string]string
	whereMap          map[string]string
	filterMap         map[string]string
	lockTables        bool
	charset           string
	quick             bool
	singleTransaction bool
	addLocks          bool
}

const (
	ExtendedInsertRows = 100
	IgnoreMapPlacement = "ignore"
	NoDataMapPlacement = "nodata"
)

func NewMySQLDumper(db *sql.DB, logger *zap.Logger, options ...Option) (MySql, error) {
	m := &mySql{
		db:                db,
		log:               logger,
		quick:             false,
		charset:           "utf8",
		singleTransaction: false,
		lockTables:        true,
		addLocks:          true,
	}

	err := parseMysqlOptions(m, options)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (d *mySql) SetSelectMap(m map[string]map[string]string) {
	d.selectMap = m
}

func (d *mySql) SetWhereMap(m map[string]string) {
	d.whereMap = m
}

func (d *mySql) SetFilterMap(noData []string, ignore []string) error {
	d.filterMap = make(map[string]string)

	t, err := d.getTables()
	if err != nil {
		return err
	}

	nd, err := d.listTables(t, noData)
	if err != nil {
		return err
	}

	for _, table := range nd {
		d.filterMap[table] = NoDataMapPlacement
	}

	ign, err := d.listTables(t, ignore)
	if err != nil {
		return err
	}

	for _, table := range ign {
		d.filterMap[table] = IgnoreMapPlacement
	}

	return nil
}

// Dump creates a MySQL dump and writes it to an io.Writer
// returns error in the event something gos wrong in the middle of the dump process
func (d *mySql) Dump(w io.Writer) error {
	_, err := fmt.Fprintf(w, "SET NAMES %s;\n", d.charset)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")
	if err != nil {
		return err
	}

	var tx *sql.Tx

	if d.singleTransaction {
		tx, err = d.db.Begin()
		if err != nil {
			return err
		}
	}

	d.log.Debug("retrieving table list")
	tables, err := d.getTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if d.filterMap[strings.ToLower(table)] != IgnoreMapPlacement {
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
			var dump string
			if dump, err = d.getCreateTableStatement(w, table); err != nil {
				return err
			}

			if !skipData {

				var cnt uint64
				dump, cnt, err = d.getTableHeader(table)
				if err != nil {
					return err
				}
				if cnt > 0 {
					if d.addLocks {
						dump = d.getLockTableWriteStatement(table)
					}

					// before the data dump, we need to flush everything to file
					if _, err = fmt.Fprintln(w, dump); err != nil {
						return err
					}
					// and after flush we need to clear the variable
					dump = ""

					if err := d.dumpTableData(w, table); err != nil {
						return err
					}

					if d.addLocks {
						dump = dump + d.getUnlockTablesStatement()
					}

					if d.lockTables {
						if _, err := d.mysqlUnlockTables(); err != nil {
							return err
						}
					}
				}
			}

			if _, err = fmt.Fprintln(w, dump); err != nil {
				d.log.Error(err.Error())
			}
		}
	}

	if d.singleTransaction {
		err = tx.Commit()
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

func (d *mySql) listTables(tables []string, globs []string) ([]string, error) {
	var globbed []string

	for _, query := range globs {
		g := glob.MustCompile(query)

		for _, table := range tables {
			if g.Match(table) {
				globbed = core.AppendIfNotExists(globbed, table)
			}
		}
	}

	return globbed, nil
}

func (d *mySql) getTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = d.db.Query("SHOW FULL TABLES"); err != nil {
		return
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			d.log.Error(err.Error(),
				zap.String("internal", "failed to close rows while getting tables"),
			)
		}
	}(rows)

	for rows.Next() {
		var tableName, tableType string

		if err = rows.Scan(&tableName, &tableType); err != nil {
			return
		}

		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}

	return
}

func (d *mySql) getCreateTableStatement(w io.Writer, table string) (string, error) {
	d.log.Debug("dumping structure for table",
		zap.String("table", table))
	s := fmt.Sprintf("\n--\n-- Structure for table `%s`\n--\n\n", table)
	s += fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table)
	row := d.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tname, ddl string
	if err := row.Scan(&tname, &ddl); err != nil {
		return "", err
	}
	s += fmt.Sprintf("%s;\n", ddl)
	return s, nil
}

func (d *mySql) dumpTableData(w io.Writer, table string) (err error) {
	d.log.Debug("dumping data for table",
		zap.String("table", table))
	rows, columns, err := d.selectAllDataFor(table)
	if err != nil {
		return
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			d.log.Error(
				err.Error(),
				zap.String("table", table),
				zap.String("context", "dumping data, closing rows failed"))
		}
	}(rows)

	numRows := ExtendedInsertRows
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
		if err = rows.Scan(scanArgs...); err != nil {
			return err
		}
		var vals []string
		for _, col := range values {
			val := "NULL"
			if col != nil {
				val = fmt.Sprintf("'%s'", escape(string(*col)))
			}

			// @todo need to inject faker usage here

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

	return
}

func (d *mySql) getTableHeader(table string) (string, uint64, error) {
	s := fmt.Sprintf("\n--\n-- Data for table `%s`", table)
	count, err := d.rowCount(table)

	if err != nil {
		return "", 0, err
	}

	s = s + fmt.Sprintf(" -- %d rows\n--\n\n", count)
	return s, count, nil
}

func (d *mySql) selectAllDataFor(table string) (rows *sql.Rows, columns []string, err error) {
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

func (d *mySql) rowCount(table string) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if where, ok := d.whereMap[strings.ToLower(table)]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	row := d.db.QueryRow(query)
	if err = row.Scan(&count); err != nil {
		return
	}
	return
}

func (d *mySql) getColumnsForSelect(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = d.db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			d.log.Warn(
				err.Error(),
				zap.String("table", table))
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

func (d *mySql) getSelectQueryFor(table string) (query string, err error) {
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

func (d *mySql) getLockTableWriteStatement(table string) string {
	return fmt.Sprintf("LOCK TABLES `%s` WRITE;\n", table)
}

func (d *mySql) getUnlockTablesStatement() string {
	return "UNLOCK TABLES;\n"
}

func (d *mySql) mysqlLockTableRead(table string) (sql.Result, error) {
	d.log.Debug("locking table",
		zap.String("table", table),
		zap.String("action", "read lock"))
	return d.db.Exec(fmt.Sprintf("LOCK TABLES `%s` READ", table))
}
func (d *mySql) mysqlFlushTable(table string) (sql.Result, error) {
	d.log.Debug("flushing table",
		zap.String("table", table),
		zap.String("action", "flush"))
	return d.db.Exec(fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// Release the global read locks
func (d *mySql) mysqlUnlockTables() (sql.Result, error) {
	d.log.Debug("unlocking tables",
		zap.String("table", "all"),
		zap.String("action", "unlock"))
	return d.db.Exec(fmt.Sprintf("UNLOCK TABLES"))
}
