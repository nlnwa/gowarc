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
	"context"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/nlnwa/gowarc/pkg/index"
	"github.com/nlnwa/gowarc/pkg/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the warc server to serve warc records",
		Long:  ``,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Increase GOMAXPROCS as recommended by badger
			// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
			runtime.GOMAXPROCS(128)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				viper.Set("warc-dir", args)
			}
			return runE()
		},
	}

	cmd.Flags().IntP("port", "p", 9999, "Server listening port")
	cmd.Flags().StringP("path-prefix", "", "/", "Path prefix")
	cmd.Flags().IntP("watch-depth", "d", 4, "The maximum depth when indexing warc")
	cmd.Flags().BoolP("auto-index", "", true, "Enable automatic indexing")

	cmd.Flags().StringP("index-dir", "", ".", "Index directory")
	cmd.Flags().StringSliceP("warc-dir", "", []string{"."}, "List of directories containing warcfiles")
	cmd.Flags().StringP("cdx-cache-size", "", "", "Size of cdx index cache in bytes")
	cmd.Flags().StringP("file-cache-size", "", "", "Size of file index cache in bytes")
	cmd.Flags().StringP("id-cache-size", "", "", "Size of id index cache")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind serve flags: %v", err)
	}

	return cmd
}

func runE() error {
	opts := index.DefaultOptions().
		WithDir(viper.GetString("index-dir")).
		WithIdCacheSize(int64(viper.GetSizeInBytes("id-cache-size"))).
		WithFileCacheSize(int64(viper.GetSizeInBytes("file-cache-size"))).
		WithCdxCacheSize(int64(viper.GetSizeInBytes("cdx-cache-size")))

	db, err := index.NewIndexDb(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if viper.GetBool("auto-index") {
		log.Infof("Starting autoindexer")
		autoindexer := index.NewAutoIndexer(db, viper.GetStringSlice("warc-dir"), viper.GetInt("watch-depth"))
		defer autoindexer.Shutdown()
	}

	loggingMw := func(h http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(os.Stdout, h)
	}
	server.Handler(db, loggingMw)

	httpServer := &http.Server{
		Addr: fmt.Sprintf(":%v", viper.GetString("port")),
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	log.Infof("Starting web server at http://localhost:%v", viper.GetInt("port"))
	return httpServer.ListenAndServe()
}
