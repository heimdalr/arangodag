package arangodag_test

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"golang.org/x/net/context"
	"strconv"
	"strings"
	"time"
)

func Example() {
	ctx := context.Background()

	// new ArangoDB-connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new ArangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.NewDAG(ctx, "test-"+uid, uid, client, true)

	// add some vertices (with explicit keys)
	_, _ = d.AddNamedVertex(ctx, "0", "blah")
	_, _ = d.AddNamedVertex(ctx, "1", "foo")
	_, _ = d.AddNamedVertex(ctx, "2", "bar")

	// add some edges
	_, _ = d.AddEdge(ctx, "0", "1", nil, false)
	_, _ = d.AddEdge(ctx, "0", "2", nil, false)

	// get size
	order, _ := d.GetOrder(ctx)
	size, _ := d.GetSize(ctx)
	dot, _ := d.String(ctx)

	// print some DAG stats and the dot graph
	fmt.Printf("order: %d\nsize: %d\n%s", order, size, trimForOutput(dot))

	// Output:
	// order: 3
	// size: 2
	// digraph  {
	//  n1[label="0"];
	//  n2[label="1"];
	//  n3[label="2"];
	//  n1->n2;
	//  n1->n3;
	// }

}

// trimForOutput is only for test purposes to ensure matching results
func trimForOutput(s string) string {
	return strings.Replace(strings.Replace(s, "\t", " ", -1), "\n \n", "\n", -1)
}
