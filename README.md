# arangoDag


[![Test](https://github.com/heimdalr/arangodag/actions/workflows/test.yml/badge.svg)](https://github.com/heimdalr/arangodag/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/heimdalr/arangodag/badge.svg?branch=main)](https://coveralls.io/github/heimdalr/arangodag?branch=main)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/heimdalr/arangodag)](https://pkg.go.dev/github.com/heimdalr/arangodag)
[![Go Report Card](https://goreportcard.com/badge/github.com/heimdalr/arangodag)](https://goreportcard.com/report/github.com/heimdalr/arangodag)

Implementation of directed acyclic graphs (DAGs) on top of ArangoDB.

## Quickstart

Running: 

``` go
package main

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"strconv"
	"time"
)

func main() {
    // new arangoDB connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new arangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.NewDAG("test-" + uid, "vertices-" + uid, "edges-" + uid, client)

	// add some vertices
	key0, _ := d.AddVertex(struct{blah string}{"0"})
	key1, _ := d.AddVertex(struct{foo string; bar string}{"1", "blub"})
	key2, _ := d.AddVertex(struct{num int}{42})

	// add some edges
	_ = d.AddEdge(key0, key1)
	_ = d.AddEdge(key0, key2)

	// get size
	order, _ := d.GetOrder()
	size, _ := d.GetSize()

	fmt.Printf("order: %d\nsize: %d", order, size)
```

will result in something like:

```
order: 3
size: 2
```