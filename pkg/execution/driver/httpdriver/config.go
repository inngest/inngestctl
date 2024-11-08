package httpdriver

import (
	"github.com/inngest/inngest/pkg/config/registration"
	"github.com/inngest/inngest/pkg/execution/driver"
)

func init() {
	registration.RegisterDriver(func() any { return &Config{} })
}

// Config represents driver configuration for use when configuring hosted
// services via config.cue
type Config struct {
	SigningKey string
	Timeout    int
}

// RuntimeName returns the runtime field that should invoke this driver.
func (Config) RuntimeName() string { return "http" }

// DriverName returns the name of this driver
func (Config) DriverName() string { return "http" }

func (c Config) NewDriver(opts ...registration.NewDriverOpts) (driver.Driver, error) {
	var skey []byte
	requireLocalSigningKey := false
	if len(opts) > 0 {
		if opts[0].LocalSigningKey != nil && opts[0].LocalSigningKey.Valid() {
			skey = []byte(opts[0].LocalSigningKey.Raw())
		}

		if opts[0].RequireLocalSigningKey {
			requireLocalSigningKey = true
		}
	}

	return &executor{
		Client:                 DefaultClient,
		localSigningKey:        skey,
		requireLocalSigningKey: requireLocalSigningKey,
	}, nil
}
