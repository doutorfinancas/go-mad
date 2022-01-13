package core

import (
	"fmt"
	"github.com/DrSmithFr/go-console/pkg/formatter"
	"github.com/DrSmithFr/go-console/pkg/style"
	"os"
	"strings"
)

type ConsoleStyler struct {
	*style.GoStyler
	isSilent bool
}

func NewConsoleStyler(isSilent bool) *ConsoleStyler {
	return &ConsoleStyler{
		GoStyler: style.NewConsoleStyler(),
		isSilent: isSilent,
	}
}

func (g *ConsoleStyler) SetSilent() {
	g.isSilent = true
}

func (g *ConsoleStyler) SetLoud() {
	g.isSilent = false
}

func (g *ConsoleStyler) HandleRuntimeException() {
	err := recover()

	if err == nil {
		// nothing append, continue
		return
	}

	msg := fmt.Sprintf("%s", err)
	full := fmt.Sprintf("%+v", err)

	traces := strings.TrimPrefix(full, msg)
	traces = strings.Replace(traces, "\n\t", "() at ", -1)

	if !g.isSilent {
		g.Error(msg)

		g.WriteOutput("<comment>Exception trace:</comment>")
		for _, trace := range strings.Split(traces, "\n") {
			g.WriteOutput(
				fmt.Sprintf(
					" %s",
					formatter.Escape(trace),
				),
			)
		}
	}

	os.Exit(2)
}

func (g *ConsoleStyler) WriteOutput(message string) {
	if g.isSilent {
		return
	}

	g.GetOutput().Write(message)
}
