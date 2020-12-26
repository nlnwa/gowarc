/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package serve

import (
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type conf struct {
	port        int
	offset      int64
	recordCount int
	header      bool
	strict      bool
	fileName    string
	id          []string
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the warc server to serve warc records",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runE(c)
		},
	}

	cmd.Flags().IntVarP(&c.port, "port", "p", -1, "the port that should be used to serve, will use config value otherwise")
	cmd.Flags().Int64VarP(&c.offset, "offset", "o", -1, "record offset")
	cmd.Flags().IntVarP(&c.recordCount, "record-count", "c", 0, "the maximum number of records to show")
	cmd.Flags().BoolVar(&c.header, "header", false, "show header")
	cmd.Flags().BoolVarP(&c.strict, "strict", "s", false, "strict parsing")
	cmd.Flags().StringArrayVar(&c.id, "id", []string{}, "id")

	return cmd
}

func runE(c *conf) error {
	if c.port < 0 {
		c.port = viper.GetInt("warcport")
	}

	dbDir := viper.GetString("indexdir")
	db, err := index.NewIndexDb(dbDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Infof("Starting autoindexer")
	if viper.GetBool("autoindex") {
		autoindexer := index.NewAutoIndexer(db)
		defer autoindexer.Shutdown()
	}

	log.Infof("Starting web server at http://localhost:%v", c.port)
	server.Serve(db, c.port)
	return nil
}
