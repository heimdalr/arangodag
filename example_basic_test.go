package arangodag_test

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"strconv"
	"time"
)

func Example() {

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

	// Output:
	// order: 3
	// size: 2
}