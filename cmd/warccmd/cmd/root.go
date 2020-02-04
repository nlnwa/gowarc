/*
Copyright © 2019 National Library of Norway

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"github.com/nlnwa/gowarc/cmd/warccmd/cmd/cat"
	"github.com/nlnwa/gowarc/cmd/warccmd/cmd/ls"
	"github.com/nlnwa/gowarc/cmd/warccmd/cmd/serve"
	"github.com/spf13/cobra"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

type conf struct {
	cfgFile string
}

// NewCommand returns a new cobra.Command implementing the root command for warccmd
func NewCommand() *cobra.Command {
	c := &conf{}
	cmd := &cobra.Command{
		Use:   "warccmd",
		Short: "A brief description of your application",
		Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		//	Run: func(cmd *cobra.Command, args []string) { },
	}

	cobra.OnInitialize(func() { c.initConfig() })

	// Flags
	cmd.PersistentFlags().StringVar(&c.cfgFile, "config", "", "config file (default is $HOME/.warccmd.yaml)")

	// Subcommands
	cmd.AddCommand(ls.NewCommand())
	cmd.AddCommand(cat.NewCommand())
	cmd.AddCommand(serve.NewCommand())

	return cmd
}

// initConfig reads in config file and ENV variables if set.
func (c *conf) initConfig() {
	if c.cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(c.cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".warccmd" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".warccmd")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
