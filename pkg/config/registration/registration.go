package registration

import (
	"context"

	"github.com/inngest/inngest/pkg/execution/driver"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/keys"
)

var (
	// registeredDrivers stores all registered driver configurations.
	registeredDrivers = map[string]func() any{}

	registeredQueues = map[string]func() any{}

	registeredStates = map[string]func() any{}

	registeredDataStores = map[string]func() any{}
)

func RegisteredDrivers() map[string]func() any {
	return registeredDrivers
}

func RegisteredQueues() map[string]func() any {
	return registeredQueues
}

func RegisteredStates() map[string]func() any {
	return registeredStates
}

func RegisteredDataStores() map[string]func() any {
	return registeredDataStores
}

// RegisterDriver registers a driver's configuration for use with
// self-hosted services.
func RegisterDriver(f func() any) {
	// Overwrite any previous drivers.
	driver := f().(DriverConfig)
	registeredDrivers[driver.DriverName()] = f
}

// RegisterQueue registers a queue for use within self hosted services/
func RegisterQueue(f func() any) {
	driver := f().(QueueConfig)
	registeredQueues[driver.QueueName()] = f
}

// RegisterState registers a state manager for use within self hosted services/
func RegisterState(f func() any) {
	driver := f().(StateConfig)
	registeredStates[driver.StateName()] = f
}

type NewDriverOpts struct {
	LocalSigningKey        *keys.SigningKey
	RequireLocalSigningKey bool
}

// DriverConfig is an interface used to determine driver config structs.
type DriverConfig interface {
	NewDriver(opts ...NewDriverOpts) (driver.Driver, error)

	// DriverName returns the name of the specific driver.
	DriverName() string
	// RuntimeName returns the name of the runtime used within the
	// driver implemetation and step configuration.
	RuntimeName() string
}

type QueueConfig interface {
	QueueName() string
	Queue() (queue.Queue, error)
	//Producer() (queue.Producer, error)
	//Consumer() (queue.Consumer, error)
}

type StateConfig interface {
	StateName() string
	SingleClusterManager(context.Context) (state.Manager, error)
}
