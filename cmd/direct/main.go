package main

import (
	"context"
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
	exists, _ := client.DatabaseExists(context.Background(), "direct")
	if exists {
		db, _ = client.Database(context.Background(), "direct")
	} else {
		db, _ = client.CreateDatabase(context.Background(), "direct", nil)
	}

	// use or create vertex collection
	var vertices driver.Collection
	exists, _ = db.CollectionExists(context.Background(), "vertices")
	if exists {
		vertices, _ = db.Collection(context.Background(), "vertices")
	} else {
		vertices, _ = db.CreateCollection(context.Background(), "vertices", nil)
	}

	type myDoc struct {
		Foo string
	}

	// add a document
	meta, _ := vertices.CreateDocument(context.Background(), myDoc{Foo: "bar"})

	var back myDoc
	_, _ = vertices.ReadDocument(context.Background(), meta.Key, &back)

	query := fmt.Sprintf("FOR d IN %s LIMIT 10 RETURN d", vertices.Name())
	cursor, _ := db.Query(context.Background(), query, nil)
	defer cursor.Close()

	fmt.Print(back)

}
