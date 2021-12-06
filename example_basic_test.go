package arangodag_test

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/heimdalr/arangodag"
	"strconv"
	"strings"
	"time"
)

type myDocument struct {
	Key  string `json:"_key"`
	Text string `json:"text"`
}

func Example() {

	// new ArangoDB-connection
	conn, _ := http.NewConnection(http.ConnectionConfig{Endpoints: []string{"http://localhost:8529"}})

	// new ArangoDB client
	client, _ := driver.NewClient(driver.ClientConfig{Connection: conn})

	// connect to DAG (create a new one if necessary)
	uid := strconv.FormatInt(time.Now().UnixNano(), 10)
	d, _ := arangodag.NewDAG("test-"+uid, uid, client)

	// add some vertices (with explicit keys)
	_, _ = d.AddVertex(myDocument{"0", "blah"})
	_, _ = d.AddVertex(myDocument{"1", "foo"})
	_, _ = d.AddVertex(myDocument{"2", "bar"})

	// add some edges
	_, _ = d.AddEdge("0", "1")
	_, _ = d.AddEdge("0", "2")

	// get size
	order, _ := d.GetOrder()
	size, _ := d.GetSize()
	dot, _ := d.String()

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
