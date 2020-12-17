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
	offset      int64
	watchDepth  int
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

	cmd.Flags().Int64VarP(&c.offset, "offset", "o", -1, "record offset")
	cmd.Flags().IntVarP(&c.watchDepth, "watch-depth", "w", 4, "The maximum depth when indexing warc")
	cmd.Flags().IntVarP(&c.recordCount, "record-count", "c", 0, "The maximum number of records to show")
	cmd.Flags().BoolVar(&c.header, "header", false, "show header")
	cmd.Flags().BoolVarP(&c.strict, "strict", "s", false, "strict parsing")
	cmd.Flags().StringArrayVar(&c.id, "id", []string{}, "id")

	return cmd
}

func runE(c *conf) error {
	dbDir := viper.GetString("indexdir")
	db, err := index.NewIndexDb(dbDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if viper.GetBool("autoindex") {
		log.Infof("Starting autoindexer")
		autoindexer := index.NewAutoIndexer(db, c.watchDepth)
		defer autoindexer.Shutdown()
	}

	log.Infof("Starting web server")
	server.Serve(db)
	return nil
}
