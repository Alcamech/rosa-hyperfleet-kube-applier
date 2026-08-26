package main

import (
	"os"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/cmd"
)

func main() {
	cmdRoot := cmd.NewCmdRoot()
	if err := cmdRoot.Execute(); err != nil {
		cmdRoot.PrintErrln(cmdRoot.ErrPrefix(), err.Error())
		os.Exit(1)
	}
}
