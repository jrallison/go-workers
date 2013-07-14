Proof of concept for creating [Resque](https://github.com/resque/resque) and
[Sidekiq](http://sidekiq.org/) compatible background workers in
[golang](http://golang.org/).

Example usage:

    package main
    
    import (
    	"github.com/jrallison/go-workers"
    )
    
    type MyJob struct {
    	workers.Job
    }
    
    func (*MyJob) Perform(message *interface{}) bool {
      // do something with your message
    	return true
    }
    
    func main() {
    	workers.Process("myqueue", &MyJob{}, 10)
    	workers.Process("myqueue2", &MyJob{}, 10)
    	workers.Run()
    }
