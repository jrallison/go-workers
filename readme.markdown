Proof of concept for creating [Sidekiq](http://sidekiq.org/) compatible
background workers in [golang](http://golang.org/).

Example usage:

    package main
    
    import (
    	"github.com/jrallison/go-workers"
    )
    
    func myJob(args *workers.Args) {
      // do something with your message
    }
    
    func main() {
      workers.Configure(map[string]string{
        "server":  "localhost:6400",
        "pool":    "20",
        "process": "1",
      })

    	workers.Process("myqueue", myJob, 10)
    	workers.Process("myqueue2", myJob, 10)

      go workers.StatsServer(8080)

    	workers.Run()
    }
