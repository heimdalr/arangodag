package main

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

func main() {

	// new connection
	conn, _ := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})

	// new client
	client, _ := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})

	// use or create database
	var db driver.Database
	exists, _ := client.DatabaseExists(nil, "direct")
	if exists {
		db, _ = client.Database(nil, "direct")
	} else {
		db, _ = client.CreateDatabase(nil, "direct", nil)
	}

	// use or create vertex collection
	var vertices driver.Collection
	exists, _ = db.CollectionExists(nil, "vertices")
	if exists {
		vertices, _ = db.Collection(nil, "vertices")
	} else {
		vertices, _ = db.CreateCollection(nil, "vertices", nil)
	}

	type myDoc struct{
		Foo string
	}

	// add a document
	meta, _ := vertices.CreateDocument(nil, myDoc{Foo: "bar"})

	var back myDoc
	_, _ = vertices.ReadDocument(nil, meta.Key, &back)


	query := fmt.Sprintf("FOR d IN %s LIMIT 10 RETURN d", vertices.Name())
	cursor, _ := db.Query(nil, query, nil)
	defer cursor.Close()



	fmt.Print(back)

}
