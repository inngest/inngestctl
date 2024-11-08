package commands

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/inngest/inngest/cmd/commands/internal/localconfig"
	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/devserver"
	"github.com/inngest/inngest/pkg/keys"
	"github.com/inngest/inngest/pkg/lite"
	itrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type FlagGroup struct {
	name string
	fs   *pflag.FlagSet
}

func NewCmdStart(rootCmd *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "start",
		Short:   "[Beta] Run Inngest as a single-node service.",
		Example: "inngest start",
		Run:     doStart,
	}

	groups := []FlagGroup{}

	baseFlags := pflag.NewFlagSet("base", pflag.ExitOnError)
	baseFlags.String("config", "", "Path to an Inngest configuration file")
	baseFlags.BoolP("help", "h", false, "Output this help information")
	baseFlags.String("host", "", "Server hostname")
	baseFlags.StringP("port", "p", "8288", "Server port")
	baseFlags.Int("poll-interval", 0, "Enable app sync polling at a specific interval in seconds. (default disabled)")
	baseFlags.Int("retry-interval", 0, "Retry interval in seconds for linear backoff. Minimum: 1.")
	baseFlags.StringSliceP("sdk-url", "u", []string{}, "SDK URLs to load functions from")
	baseFlags.Int("tick", devserver.DefaultTick, "Interval, in milliseconds, of which to check for new work.")
	baseFlags.String("signing-key", "", "Signing key used to sign and validate data between the server and apps.")
	baseFlags.StringSlice("event-key", []string{}, "Event key(s) that will be used by apps to send events to the server.")
	cmd.Flags().AddFlagSet(baseFlags)
	groups = append(groups, FlagGroup{name: "Flags:", fs: baseFlags})

	persistenceFlags := pflag.NewFlagSet("persistence", pflag.ExitOnError)
	persistenceFlags.String("sqlite-dir", "", "Directory for where to write SQLite database.")
	persistenceFlags.String("redis-uri", "", "Redis server URI for external queue and run state. Defaults to self-contained, in-memory Redis server with periodic snapshot backups.")
	// persistenceFlags.String("postgres-uri", "", "[Experimental] PostgreSQL database URI for configuration and history persistence. Defaults to SQLite database.")
	cmd.Flags().AddFlagSet(persistenceFlags)
	groups = append(groups, FlagGroup{name: "Persistence Flags:", fs: persistenceFlags})

	// Also add global flags
	groups = append(groups, FlagGroup{name: "Group Flags:", fs: rootCmd.PersistentFlags()})

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Printf("%s\n  %s\n\n%s\n%s\n\n",
			"Usage:",
			"inngest start [flags]",
			"Examples:",
			"inngest start",
		)

		for _, group := range groups {
			usage := group.fs.FlagUsages()

			help := ""
			if group.name != "" {
				help = help + group.name + "\n"
			}
			help = help + usage
			fmt.Println(help)
		}

		return nil
	})

	return cmd
}

func doStart(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	// TODO Likely need a `Start()`
	conf, err := config.Dev(ctx)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if err = localconfig.InitStartConfig(ctx, cmd); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	port, err := strconv.Atoi(viper.GetString("port"))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	conf.EventAPI.Port = port

	host := viper.GetString("host")
	if host != "" {
		conf.EventAPI.Addr = host
	}

	if err := itrace.NewUserTracer(ctx, itrace.TracerOpts{
		ServiceName:   "tracing",
		TraceEndpoint: "localhost:8288",
		TraceURLPath:  "/dev/traces",
		Type:          itrace.TracerTypeOTLPHTTP,
	}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() {
		_ = itrace.CloseUserTracer(ctx)
	}()

	tick := viper.GetInt("tick")
	if tick < 1 {
		tick = devserver.DefaultTick
	}

	sk, _ := keys.NewSigningKey(viper.GetString("signing-key"))

	opts := lite.StartOpts{
		Config:        *conf,
		PollInterval:  viper.GetInt("poll-interval"),
		RedisURI:      viper.GetString("redis-uri"),
		RetryInterval: viper.GetInt("retry-interval"),
		Tick:          time.Duration(tick) * time.Millisecond,
		URLs:          viper.GetStringSlice("sdk-url"),
		SQLiteDir:     viper.GetString("sqlite-dir"),
		SigningKey:    sk,
		EventKey:      viper.GetStringSlice("event-key"),
	}

	err = lite.New(ctx, opts)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
