package main

import (
	"github.com/DrSmithFr/go-console/pkg/input/argument"
	"github.com/DrSmithFr/go-console/pkg/input/option"
	"github.com/doutorfinancas/go-mad/core"
)

func main() {
	io := setupConsole()

	if option.DEFINED == io.GetInput().GetOption("quiet") {
		io.SetSilent()
	}

	defer io.HandleRuntimeException()

}

func setupConsole() *core.ConsoleStyler {
	io := core.NewConsoleStyler(false)
	io.AddInputOption(
		option.
			New("host", option.OPTIONAL).
			SetShortcut("h")).
		AddInputOption(
			option.
				New("user", option.OPTIONAL).
				SetShortcut("u"),
		).
		AddInputOption(
			option.
				New("password", option.OPTIONAL).
				SetShortcut("p"),
		).
		AddInputOption(
			option.
				New("port", option.OPTIONAL).
				SetShortcut("P"),
		).
		AddInputOption(
			option.
				New("quiet", option.NONE).
				SetShortcut("q"),
		).
		AddInputArgument(
			argument.
				New("database", argument.OPTIONAL|argument.IS_ARRAY),
		).
		ParseInput().
		ValidateInput()

	return io
}
