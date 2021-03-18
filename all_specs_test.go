package workers

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/customerio/gospec"
)

// You will need to list every spec in a TestXxx method like this,
// so that gotest can be used to run the specs. Later GoSpec might
// get its own command line tool similar to gotest, but for now this
// is the way to go. This shouldn't require too much typing, because
// there will be typically only one top-level spec per class/feature.

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()

	r.Parallel = false
	redisNodes := 6

	r.BeforeEach = func() {
		mrs := []*miniredis.Miniredis{}
		serverAddrs := []string{}
		for i := 0; i < redisNodes; i++ {
			mr, _ := miniredis.Run()
			mrs = append(mrs, mr)
			serverAddrs = append(serverAddrs, mr.Addr())
		}

		Configure(map[string]string{
			"server":  strings.Join(serverAddrs[:], ","),
			"process": "1",
		})

		for i := 0; i < redisNodes; i++ {
			mrs[i].FlushAll()
		}
	}

	// List all specs here
	r.AddSpec(WorkersSpec)
	r.AddSpec(ConfigSpec)
	r.AddSpec(MsgSpec)
	r.AddSpec(FetchSpec)
	r.AddSpec(WorkerSpec)
	r.AddSpec(ManagerSpec)
	r.AddSpec(ScheduledSpec)
	r.AddSpec(EnqueueSpec)
	r.AddSpec(MiddlewareSpec)
	r.AddSpec(MiddlewareRetrySpec)
	r.AddSpec(MiddlewareStatsSpec)

	// Run GoSpec and report any errors to gotest's `testing.T` instance
	gospec.MainGoTest(r, t)
}
