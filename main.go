package main

import (
	"log"

	"github.com/doutorfinancas/go-mad/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		log.Fatal(err.Error())
	}
}
