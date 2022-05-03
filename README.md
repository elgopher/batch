[![Build](https://github.com/elgopher/batch/actions/workflows/build.yml/badge.svg)](https://github.com/elgopher/batch/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/elgopher/batch.svg)](https://pkg.go.dev/github.com/elgopher/batch)
[![Go Report Card](https://goreportcard.com/badge/github.com/elgopher/batch)](https://goreportcard.com/report/github.com/elgopher/batch)
[![codecov](https://codecov.io/gh/elgopher/batch/branch/master/graph/badge.svg)](https://codecov.io/gh/elgopher/batch)
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

## What it can be used for?

To speed up application performance **without** sacrificing *data consistency* and *data durability* or making source code/architecture complex.

The **batch** package simplifies writing Go applications that process incoming requests (HTTP, GRPC etc.) in a batch manner:
instead of processing each request separately, they group incoming requests to a batch and run whole group at once.
This method of processing can significantly speed up the application and reduce the consumption of disk, network or CPU.

The **batch** package can be used to write any type of *servers* that handle thousands of requests per second. 
Thanks to this small library, you can create relatively simple code without the need to use low-level data structures.

## Why batch processing improves performance?

Normally a web application is using following pattern to modify data in the database:

1. **Load resource** from database. Resource is some portion of data 
such as set of records from relational database, document from Document-oriented database or value from KV store.
Lock the entire resource pessimistically or optimistically (by reading version number).
2. **Apply change** to data
3. **Save resource** to database. Release the pessimistic lock. Or run
atomic update with version check (optimistic lock).

But such architecture does not scale well if number of requests 
for a single resource is very high
(meaning hundreds or thousands of requests per second). 
The lock contention in such case is very high and database is significantly 
overloaded. Practically, the number of concurrent requests is limited.  

One solution to this problem is to reduce the number of costly operations.
Because a single resource is loaded and saved thousands of times per second 
we can instead:

1. Load the resource **once** (let's say once per second) 
2. Execute all the requests from this period of time on an already loaded resource. Run them all sequentially to keep things simple and data consistent.
3. Save the resource and send responses to all clients if data was stored successfully.

Such solution could improve the performance by a factor of 1000. And resource is still stored in a consistent state. 

The **batch** package does exactly that. You configure the duration of window, provide functions
to load and save resource and once the request comes in - you run a function:

```go
// Set up the batch processor:
processor := batch.StartProcessor(
    batch.Options[*YourResource]{ // YourResource is your own Go struct
        MinDuration:  100 * time.Millisecond,
        LoadResource: func(ctx context.Context, resourceKey string) (*YourResource, error){
            // resourceKey uniquely identifies the resource
            ...
        },
        SaveResource: ...,
    },
)

// And use the processor inside http/grpc handler or technology-agnostic service.
// ResourceKey can be taken from request parameter.
err := s.BatchProcessor.Run(resourceKey, func(r *YourResource) {
    // Here you put the code which will executed sequentially inside batch  
})
```

For real-life example see [example web application](_example).

## Installation

```sh
# Add batch to your Go module:
go get github.com/elgopher/batch
```
Please note that at least **Go 1.18** is required. The package is using generics, which was added in 1.18. 

## Scaling out

Single Go http server is able to handle up to tens of thousands of requests per second on a commodity hardware. 
This is a lot, but very often you also need:

* high availability (if one server goes down you want other to handle the traffic)
* you want to handle hundred-thousands or millions of requests per second

For both cases you need to deploy **multiple servers** and put a **load balancer** in front of them. 
Please note though, that you have to carefully configure the load balancing algorithm. 
_Round-robin_ is not an option here, because sooner or later you will have problems with locking 
(multiple server instances will run batches on the same resource). 
Ideal solution is to route requests based on parameters or URL. 
For example some http parameter could be a resource key. You can instruct load balancer
to calculate hash on this parameter value and always route requests with this param value 
to the same backend (of course if all backends are still available).