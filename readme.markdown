Proof of concept for creating [Resque](https://github.com/resque/resque) and
[Sidekiq](http://sidekiq.org/) compatible background workers in
[golang](http://golang.org/).

Example usage:

    package main
    
    import (
    	"github.com/jrallison/go-workers"
    )
    
    func myJob(message *workers.Msg) bool {
      // do something with your message
      return true
    }
    
    func main() {
      workers.Configure(map[string]string{
        "server":  "localhost:6400",
        "pool":    "20",
        "process": "1",
      })

    	workers.Process("myqueue", myJob, 10)
    	workers.Process("myqueue2", myJob, 10)
    	workers.Run()
    }


TODO:

* retries
* sync message format with resque/sidekiq
* improve logging / error handling
* listen for signals to stop processing
* expose status / metrics
