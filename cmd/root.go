package cmd

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/doutorfinancas/go-mad/core"
	"github.com/doutorfinancas/go-mad/database"
	"github.com/doutorfinancas/go-mad/generator"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	Version = "0.3.2"
)

var rootCmd = &cobra.Command{
	Use:   "go-mad",
	Short: "MySQL Anonymized Dump",
	Long: `A full fledged anonymized dump facility that allows some compatibility
				with mysql original flags for mysqldump`,
	Run: func(cmd *cobra.Command, args []string) {
		if getVersion {
			fmt.Printf(
				`go-mad: Mysql Anonymized Dump version %s
	Made with love by Doutor Finanças
	We trully hope this helps:)
`,
				Version,
			)
			os.Exit(0)
		}

		logger, _ := zap.NewProduction()

		if debug {
			logger, _ = zap.NewDevelopment()
		}

		defer func(logger *zap.Logger) {
			err := logger.Sync()
			if err != nil &&
				(!strings.Contains(err.Error(), "invalid argument") && !strings.Contains(
					err.Error(),
					"inappropriate ioctl for device",
				)) {
				logger.Fatal(
					err.Error(),
					zap.String("step", "logger finalization"),
				)
			}
		}(logger) // flushes buffer, if any

		if len(args) != 1 {
			logger.Fatal(
				"database is required",
				zap.String("step", "arguments initialization"),
			)
		}

		if pwd == "" && cmd.PersistentFlags().Changed("password") {
			validate := func(input string) error {
				if len(input) < 1 {
					logger.Fatal(
						"password flag is set, so it is required",
						zap.String("step", "arguments initialization"),
					)
				}
				return nil
			}

			prompt := promptui.Prompt{
				Label:    "Password",
				Validate: validate,
				Mask:     '*',
			}

			res, err := prompt.Run()
			if err != nil {
				logger.Fatal(
					"password flag was set and was failed to be parsed",
					zap.String("step", "arguments initialization"),
				)
			}

			pwd = res
		}

		cfg := database.NewConfig(user, pwd, hostname, port, args[0])

		db, err := sql.Open("mysql", cfg.ConnectionString())
		if err != nil {
			logger.Fatal(
				err.Error(),
				zap.String("step", "database initialization"),
			)
		}

		service := generator.NewService()
		var opt []database.Option

		if quick {
			opt = append(opt, database.OptionValue("quick", ""))
		}

		if hexEncode {
			opt = append(opt, database.OptionValue("hex-encode", ""))
		}

		if ignoreGenerated {
			opt = append(opt, database.OptionValue("ignore-generated", ""))
		}

		if charset != "" {
			opt = append(opt, database.OptionValue("set-charset", charset))
		}

		if triggerDelimiter != "" {
			opt = append(opt, database.OptionValue("trigger-delimiter", triggerDelimiter))
		}

		if singleTransaction {
			opt = append(opt, database.OptionValue("single-transaction", ""))
			// if we do single-transaction we need to automatically turn skip-lock-tables on
			// since we rely on FLUSH TABLES `table` WITH READ LOCK, which implicitly commits work
			// it would ruin the fact that we are within a transaction, by possibly sneaking in data
			// https://dev.mysql.com/doc/refman/8.0/en/flush.html
			opt = append(opt, database.OptionValue("skip-lock-tables", ""))
		}

		if skipLockTables && !singleTransaction {
			opt = append(opt, database.OptionValue("skip-lock-tables", ""))
		}

		if dumpTrigger {
			opt = append(opt, database.OptionValue("dump-trigger", ""))
		}

		if skipDefiner {
			opt = append(opt, database.OptionValue("skip-definer", ""))
		}

		if parallel {
			opt = append(opt, database.OptionValue("parallel", ""))
		}

		dumper, err := database.NewMySQLDumper(db, logger, service, opt...)
		if err != nil {
			logger.Fatal(
				err.Error(),
				zap.String("step", "config initialization"),
			)
		}

		if configFilePath != "" {
			d, dErr := os.ReadFile(configFilePath)
			if dErr != nil {
				logger.Fatal(
					dErr.Error(),
					zap.String("step", "config initialization"),
				)
			}

			pConf, loadErr := core.Load(d)
			if loadErr != nil {
				logger.Fatal(
					loadErr.Error(),
					zap.String("step", "config loading"),
				)
			}
			dumper.SetSelectMap(pConf.RewriteToMap())
			dumper.SetWhereMap(pConf.Where)
			if dErr := dumper.SetFilterMap(pConf.NoData, pConf.Ignore); dErr != nil {
				logger.Fatal(
					dErr.Error(),
					zap.String("step", "config loading"),
				)
			}
		}

		var w io.Writer

		if outputPath == "stdout" {
			w = os.Stdout
		} else {
			if w, err = os.Create(outputPath); err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "file initialization"),
				)
			}
		}

		if err = dumper.Dump(w); err != nil {
			logger.Error(
				err.Error(),
				zap.String("step", "dump process"),
			)
		}
	},
}

var (
	user              string
	pwd               string
	hostname          string
	port              string
	charset           string
	configFilePath    string
	outputPath        string
	skipLockTables    bool
	quick             bool
	singleTransaction bool
	addLocks          bool
	debug             bool
	quiet             bool
	hexEncode         bool
	ignoreGenerated   bool
	getVersion        bool
	insertIntoLimit   string
	dumpTrigger       bool
	skipDefiner       bool
	triggerDelimiter  string
	parallel          bool
)

func Execute() error {
	return rootCmd.Execute()
}

// nolint
func init() {
	rootCmd.PersistentFlags().StringVarP(
		&hostname,
		"host",
		"H",
		"127.0.0.1",
		"hostname or ip where machine is located",
	)

	rootCmd.PersistentFlags().StringVarP(
		&pwd,
		"password",
		"p",
		"this_is_an_empty_password",
		"insert you password, though direct command line usage is supported, it can be insecure",
	)

	rootCmd.PersistentFlags().StringVarP(
		&user,
		"user",
		"u",
		"root",
		"username to connect to the database",
	)

	rootCmd.PersistentFlags().StringVarP(
		&port,
		"port",
		"P",
		"3306",
		"port to connect to the database",
	)

	rootCmd.PersistentFlags().StringVarP(
		&configFilePath,
		"config",
		"c",
		"",
		"filepath to configuration",
	)

	rootCmd.PersistentFlags().StringVarP(
		&outputPath,
		"output",
		"o",
		"stdout",
		"filepath to output, defaults to stdout",
	)

	rootCmd.PersistentFlags().StringVar(
		&charset,
		"set-charset",
		"utf8",
		"chartset to be used for database connection and respective dump",
	)

	rootCmd.PersistentFlags().BoolVarP(
		&debug,
		"debug",
		"v",
		false,
		"use verbose mode",
	)

	rootCmd.PersistentFlags().BoolVarP(
		&quiet,
		"quiet",
		"q",
		false,
		"use silent mode",
	)

	rootCmd.PersistentFlags().BoolVar(
		&skipLockTables,
		"skip-lock-tables",
		false,
		"use silent mode",
	)

	rootCmd.PersistentFlags().BoolVar(
		&singleTransaction,
		"single-transaction",
		false,
		"use silent mode",
	)

	rootCmd.PersistentFlags().BoolVar(
		&quick,
		"quick",
		false,
		"dump rows one line at a time, useful for large tables",
	)

	rootCmd.PersistentFlags().BoolVar(
		&getVersion,
		"version",
		false,
		"returns version for go-mad",
	)

	rootCmd.PersistentFlags().BoolVar(
		&addLocks,
		"add-locks",
		false,
		"add lock statements to dump",
	)

	rootCmd.PersistentFlags().BoolVar(
		&hexEncode,
		"hex-encode",
		false,
		"performs hex encoding and respective decode statement for binary values",
	)

	rootCmd.PersistentFlags().BoolVar(
		&ignoreGenerated,
		"ignore-generated",
		false,
		"strips generated columns from create statements",
	)

	rootCmd.PersistentFlags().StringVar(
		&insertIntoLimit,
		"insert-into-limit",
		"100",
		"limit for the number of rows to go into each insert, cannot be used in conjunction with quick",
	)

	rootCmd.PersistentFlags().BoolVar(
		&dumpTrigger,
		"dump-trigger",
		false,
		"dump triggers",
	)

	rootCmd.PersistentFlags().BoolVar(
		&skipDefiner,
		"skip-definer",
		false,
		"skip definer in dumped triggers",
	)

	rootCmd.PersistentFlags().StringVar(
		&triggerDelimiter,
		"trigger-delimiter",
		"",
		"define the char to delimit triggers",
	)

	rootCmd.PersistentFlags().BoolVar(
		&parallel,
		"parallel",
		false,
		"run exports in parallel",
	)
}
