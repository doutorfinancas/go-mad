package cmd

import (
	"database/sql"
	"github.com/doutorfinancas/go-mad/core"
	"github.com/doutorfinancas/go-mad/database"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "dump",
	Short: "MySql Anonymized Dump",
	Long: `A full fledged anonymized dump facility that allows some compatibility
				with mysql original flags for mysqldump`,
	Run: func(cmd *cobra.Command, args []string) {
		logger, _ := zap.NewProduction()

		if debug {
			logger, _ = zap.NewDevelopment()
		}

		defer func(logger *zap.Logger) {
			err := logger.Sync()
			if err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "logger initialization"))
			}
		}(logger) // flushes buffer, if any

		if len(args) != 1 {
			logger.Fatal(
				"database is required",
				zap.String("step", "arguments initialization"))
		}

		logger.Sugar()

		if pwd == "" && cmd.PersistentFlags().Changed("password") {
			validate := func(input string) error {
				if len(input) < 1 {
					logger.Fatal(
						"password flag is set, so it is required",
						zap.String("step", "arguments initialization"))
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
					zap.String("step", "arguments initialization"))
			}

			pwd = res
		}

		cfg := database.NewConfig(user, pwd, hostname, port, args[0])

		db, err := sql.Open("mysql", cfg.ConnectionString())
		if err != nil {
			logger.Fatal(
				err.Error(),
				zap.String("step", "database initialization"))
		}

		dumper, err := database.NewMySQLDumper(db, logger)

		if configFilePath != "" {
			d, err := ioutil.ReadFile(configFilePath)
			if err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "config initialization"))
			}

			pConf, err := core.Load(d)
			if err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "config loading"))
			}
			dumper.SetSelectMap(pConf.RewriteToMap())
			dumper.SetWhereMap(pConf.WhereToMap())
			if err := dumper.SetFilterMap(pConf.NoData, pConf.Ignore); err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "config loading"))
			}
		}

		var w io.Writer

		if outputPath == "stdout" {
			w = os.Stdout
		} else {
			if w, err = os.Create(outputPath); err != nil {
				logger.Fatal(
					err.Error(),
					zap.String("step", "file initialization"))
			}
		}

		if err = dumper.Dump(w); err != nil {
			logger.Error(
				err.Error(),
				zap.String("step", "dump process"))
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
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&hostname,
		"host",
		"h",
		"127.0.0.1",
		"hostname or ip where machine is located")

	rootCmd.PersistentFlags().StringVarP(
		&pwd,
		"password",
		"p",
		"this_is_an_empty_password",
		"insert you password, though direct command line usage is supported, it can be insecure")

	rootCmd.PersistentFlags().StringVarP(
		&user,
		"user",
		"u",
		"",
		"username to connect to the database")

	rootCmd.PersistentFlags().StringVarP(
		&port,
		"port",
		"P",
		"3306",
		"port to connect to the database")

	rootCmd.PersistentFlags().StringVarP(
		&configFilePath,
		"config",
		"c",
		"",
		"filepath to configuration")

	rootCmd.PersistentFlags().StringVarP(
		&outputPath,
		"output",
		"o",
		"stdout",
		"filepath to output, defaults to stdout")

	rootCmd.PersistentFlags().StringVar(
		&charset,
		"set-charset",
		"utf8",
		"chartset to be used for database connection and respective dump")

	rootCmd.PersistentFlags().BoolVarP(
		&debug,
		"debug",
		"v",
		false,
		"use verbose mode")

	rootCmd.PersistentFlags().BoolVarP(
		&quiet,
		"quiet",
		"q",
		false,
		"use silent mode")

	rootCmd.PersistentFlags().BoolVar(
		&skipLockTables,
		"skip-lock-tables",
		false,
		"use silent mode")

	rootCmd.PersistentFlags().BoolVar(
		&singleTransaction,
		"single-transaction",
		false,
		"use silent mode")

	rootCmd.PersistentFlags().BoolVar(
		&quick,
		"quick",
		false,
		"dump rows one line at a time, useful for large tables")

	rootCmd.PersistentFlags().BoolVar(
		&addLocks,
		"add-locks",
		false,
		"add lock statements to dump")
}
