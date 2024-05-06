package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/devserver"
	"github.com/inngest/inngest/pkg/telemetry"
	"github.com/spf13/cobra"
)

func NewCmdDev() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "dev",
		Short:   "Run the Inngest dev server",
		Example: "inngest dev -u http://localhost:3000/api/inngest",
		Run:     doDev,
	}

	cmd.Flags().String("host", "", "host to run the API on")
	cmd.Flags().StringP("port", "p", "8288", "port to run the API on")
	cmd.Flags().StringSliceP("sdk-url", "u", []string{}, "SDK URLs to load functions from")
	cmd.Flags().Bool("no-discovery", false, "Disable autodiscovery")
	cmd.Flags().Bool("no-poll", false, "Disable polling of apps for updates")
	cmd.Flags().Int("retry-interval", 0, "Retry interval in seconds for linear backoff when retrying functions - must be 1 or above")

	cmd.Flags().Int("tick", 150, "The interval (in milliseconds) at which the executor checks for new work, during local development")

	return cmd
}

func doDev(cmd *cobra.Command, args []string) {

	go func() {
		ctx, cleanup := signal.NotifyContext(
			context.Background(),
			os.Interrupt,
			syscall.SIGTERM,
			syscall.SIGINT,
			syscall.SIGQUIT,
		)
		defer cleanup()
		<-ctx.Done()
		os.Exit(0)
	}()

	ctx := cmd.Context()
	conf, err := config.Dev(ctx)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	port, err := strconv.Atoi(cmd.Flag("port").Value.String())
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	conf.EventAPI.Port = port

	host := cmd.Flag("host").Value.String()
	if host != "" {
		conf.EventAPI.Addr = host
	}

	urls, _ := cmd.Flags().GetStringSlice("sdk-url")

	for _, url := range urls {
		if strings.HasSuffix(strings.ToLower(url), "/ingest") {
			fmt.Println(fmt.Sprintf("oops! you're using an Inngest endpoint at '%s', "+
				"but 'inngest' is spelled wrong -- please spell it '/inngest'",
				url))
		}
	}

	// Run auto-discovery unless we've explicitly disabled it.
	noDiscovery, _ := cmd.Flags().GetBool("no-discovery")
	noPoll, _ := cmd.Flags().GetBool("no-poll")
	retryInterval, _ := cmd.Flags().GetInt("retry-interval")
	tick, _ := cmd.Flags().GetInt("tick")

	if err := telemetry.NewUserTracer(ctx, telemetry.TracerOpts{
		ServiceName: "devserver",
		Type:        telemetry.TracerTypeOTLPHTTP,
	}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() {
		_ = telemetry.CloseUserTracer(ctx)
	}()

	opts := devserver.StartOpts{
		Config:        *conf,
		URLs:          urls,
		Autodiscover:  !noDiscovery,
		Poll:          !noPoll,
		RetryInterval: retryInterval,
		Tick:          time.Duration(tick) * time.Millisecond,
	}

	err = devserver.New(ctx, opts)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
