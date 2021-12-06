package main

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// vertex type with self selected key
type myVertex struct {
	Key string `json:"_key"`
}

func main() {
	// new ArangoDB-connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new ArangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.NewDAG("test-"+uid, uid, client)

	createLarge(d)
	getDescendants(d)
}

func createLarge(d *arangodag.DAG) {
	start := time.Now()
	_, _ = d.AddVertex(myVertex{"0"})
	var vertexCount, edgeCount int32
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		largeAux(d, 5, 9, "0", 0, &vertexCount, &edgeCount, &wg)
		wg.Done()
	}()
	wg.Wait()
	end := time.Now()
	fmt.Printf("%fs to add %d vertices and %d edges\n", end.Sub(start).Seconds(), vertexCount+1, edgeCount)
	// with checked edges 11.055900s to add 7381 vertices and 7380 edges
	// with unchecked edges 7.362412s to add 7381 vertices and 7380 edges
	// with unchecked edges and go routines 1.447144s to add 7381 vertices and 7380 edges
}

func getDescendants(d *arangodag.DAG) {
	start := time.Now()
	var descendantsCount int32
	cursor, _ := d.GetDescendants("0", false)
	defer func() {
		_ = cursor.Close()
	}()
	ctx := context.Background()
	for {
		var vertex myVertex
		_, errRead := cursor.ReadDocument(ctx, &vertex)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			panic(errRead)
		}
		descendantsCount += 1
	}
	end := time.Now()
	fmt.Printf("%fs to get %d descandents\n", end.Sub(start).Seconds(), descendantsCount)
	// 0.106641s to get 7380 descandents
	// 1.133783s to get 66429 descandents
}

func largeAux(d *arangodag.DAG, level, branches int, parentKey string, parentValue int, vertexCount, edgeCount *int32, wg *sync.WaitGroup) {
	if level > 1 {
		for i := 1; i <= branches; i++ {
			value := parentValue*10 + i
			key := strconv.Itoa(value)
			if _, err := d.AddVertex(myVertex{key}); err != nil {
				panic(err)
			}
			atomic.AddInt32(vertexCount, 1)
			if _, err := d.AddEdgeUnchecked(parentKey, key); err != nil {
				panic(err)
			}
			atomic.AddInt32(edgeCount, 1)
			wg.Add(1)
			go func(i int) {
				largeAux(d, level-1, branches, key, value, vertexCount, edgeCount, wg)
				wg.Done()
			}(i)
		}
	}
}
