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

	// new arangoDB connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new arangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.NewDAG("test-"+uid, "vertices-"+uid, "edges-"+uid, client)

	// vertex type with self selected key
	type idVertex struct {
		Key string `json:"_key"`
	}

	// add some vertices and edges
	_, _ = d.AddVertex(idVertex{"0"})
	for i := 1; i < 10; i++ {
		dstKey := strconv.Itoa(i)
		_, _ = d.AddVertex(idVertex{dstKey})
		_, _ = d.AddEdge("0", dstKey)
	}

	// get order and size
	order, _ := d.GetOrder()
	size, _ := d.GetSize()
	fmt.Printf("order: %d\nsize: %d\n", order, size)

	fmt.Printf("children:\n")
	cursor, _ := d.GetChildren("0")
	defer func() {
		_ = cursor.Close()
	}()
	ctx := context.Background()
	var vertex idVertex
	for {
		_, errRead := cursor.ReadDocument(ctx, &vertex)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			panic(errRead)
		}
		fmt.Printf("- %s\n", vertex.Key)
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
