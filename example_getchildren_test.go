package arangodag_test

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"strconv"
	"time"
)

func ExampleDAG_GetChildren() {

	ctx := context.Background()

	// new arangoDB connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new arangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.CreateDAG(ctx, "test-"+uid, uid, client)

	// add some vertices and edges
	_, _ = d.AddNamedVertex(ctx, "0", nil)
	for i := 1; i < 10; i++ {
		dstKey := strconv.Itoa(i)
		_, _ = d.AddNamedVertex(ctx, dstKey, nil)
		_, _ = d.AddEdge(ctx, "0", dstKey, nil, false)
	}

	// get order and size
	order, _ := d.GetOrder(ctx)
	size, _ := d.GetSize(ctx)
	fmt.Printf("order: %d\nsize: %d\n", order, size)

	fmt.Printf("children:\n")
	cursor, _ := d.GetChildren(ctx, "0")
	defer func() {
		_ = cursor.Close()
	}()
	for {
		meta, errRead := cursor.ReadDocument(ctx, &struct{}{})
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			panic(errRead)
		}
		fmt.Printf("- %s\n", meta.Key)
	}

	// Unordered output:
	// order: 10
	// size: 9
	// children:
	// - 8
	// - 7
	// - 6
	// - 3
	// - 5
	// - 9
	// - 1
	// - 2
	// - 4
}
