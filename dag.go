package arangodag

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
)

// IDInterface describes the interface a type must implement in order to
// explicitly specify vertex keys.
//
// Objects of types not implementing this interface will receive automatically
// generated keys (as of adding them to the graph).
type IDInterface interface {
	ID() string
}

// DAG implements the data structure of the DAG.
type DAG struct {
	vertices driver.Collection
	edges    driver.Collection
	graph    driver.Graph
	client   driver.Client
}

// NewDAG creates / initializes a new DAG.
func NewDAG(dbName, vertexCollName, edgeCollName, graphName string, client driver.Client) (*DAG, error) {

	// use or create database
	var db driver.Database
	exists, err := client.DatabaseExists(context.Background(), dbName)
	if err != nil {
		return nil, ArangoError(err)
	}
	if exists {
		db, err = client.Database(context.Background(), dbName)
	} else {
		db, err = client.CreateDatabase(context.Background(), dbName, nil)
	}
	if err != nil {
		return nil, ArangoError(err)
	}

	// use or create vertex collection
	var vertices driver.Collection
	exists, err = db.CollectionExists(context.Background(), vertexCollName)
	if err != nil {
		return nil, ArangoError(err)
	}
	if exists {
		vertices, err = db.Collection(context.Background(), vertexCollName)
	} else {
		vertices, err = db.CreateCollection(context.Background(), vertexCollName, nil)
	}
	if err != nil {
		return nil, ArangoError(err)
	}

	// use or create edge collection
	var edges driver.Collection
	exists, err = db.CollectionExists(context.Background(), edgeCollName)
	if err != nil {
		return nil, ArangoError(err)
	}
	if exists {
		edges, err = db.Collection(context.Background(), edgeCollName)
	} else {
		options := &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		edges, err = db.CreateCollection(context.Background(), edgeCollName, options)
	}
	if err != nil {
		return nil, ArangoError(err)
	}

	// use or create graph
	edgeDefinition := driver.EdgeDefinition{
		Collection: edgeCollName,
		To:         []string{vertexCollName},
		From:       []string{vertexCollName},
	}
	graphOptions := driver.CreateGraphOptions{
		EdgeDefinitions: []driver.EdgeDefinition{edgeDefinition},
	}
	graph, err := db.CreateGraph(context.Background(), graphName, &graphOptions)
	if err != nil {
		return nil, ArangoError(err)
	}

	return &DAG{vertices: vertices, edges: edges, graph: graph, client: client}, nil
}

type arangoDocContainer struct {
	Payload interface{} `json:"payload"`
}
type arangoDocKeyContainer struct {
	Key     string      `json:"_key"`
	Payload interface{} `json:"payload"`
}

// AddVertex adds the given vertex to the DAG and returns its id. AddVertex
// returns an error, if the vertex is nil. If the vertex implements the
// IDInterface, the key will be taken from the vertex (itself). In this case,
// AddVertex returns an error, if the extracted id is empty or already exists.
func (d *DAG) AddVertex(vertex interface{}) (string, error) {

	// sanity checking
	if vertex == nil {
		return "", VertexNilError()
	}

	var doc interface{}
	var id string
	if i, ok := vertex.(IDInterface); ok {
		id = i.ID()
		doc = &arangoDocKeyContainer{Payload: vertex, Key: id}
	} else {
		doc = &arangoDocContainer{Payload: vertex}
		id = ""
	}

	ctx := driver.WithQueryCount(context.Background())
	meta, err := d.vertices.CreateDocument(ctx, doc)
	if err != nil {
		return "", ArangoError(err)
	}
	return meta.Key, nil
}

// EnsureVertex returns an error if the given id is empty or a vertex with the
// given id does not exist.
func (d *DAG) EnsureVertex(id string) error {
	if id == "" {
		return EmptyIDError()
	}
	exists, err := d.vertices.DocumentExists(context.Background(), id)
	if err != nil {
		return ArangoError(err)
	}
	if !exists {
		return UnknownIDError(id)
	}
	return nil
}

// GetVertex returns the vertex with the given id. GetVertex returns an error, if
// id is empty or unknown.
func (d *DAG) GetVertex(id string, vertex interface{}) error {
	if err := d.EnsureVertex(id); err != nil {
		return err
	}

	doc := arangoDocContainer{Payload: vertex}
	if _, err := d.vertices.ReadDocument(context.Background(), id, &doc); err != nil {
		return ArangoError(err)
	}
	return nil
}

func (d *DAG) DeleteVertex(id string) error {
	// TODO: implement
	panic("implement me")
}

type arangoEdgeContainer struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}

// AddEdge adds an edge between srcID and dstID. AddEdge returns an
// error, if srcID or dstID are empty or unknown, if the edge
// already exists, or if the new edge would create a loop.
func (d *DAG) AddEdge(srcID, dstID string) (string, error) {
	isEdge, err := d.IsEdge(srcID, dstID)
	if err != nil {
		return "", err
	}
	if isEdge {
		return "", DuplicateEdgeError(srcID, dstID)
	}
	meta, err := d.edges.CreateDocument(context.Background(), arangoEdgeContainer{
		From: fmt.Sprintf("%s/%s", d.vertices.Name(), srcID),
		To:   fmt.Sprintf("%s/%s", d.vertices.Name(), dstID),
	})
	if err != nil {
		return "", ArangoError(err)
	}
	return meta.Key, nil
}

// IsEdge returns true, if there is an edge between srcID and dstID. IsEdge
// returns an error, if srcID or dstID are empty, unknown, or the same.
func (d *DAG) IsEdge(srcID, dstID string) (bool, error) {
	if srcID == dstID {
		return false, SrcDstEqualError(srcID)
	}
	if err := d.EnsureVertex(srcID); err != nil {
		return false, err
	}
	if err := d.EnsureVertex(dstID); err != nil {
		return false, err
	}
	ctx := driver.WithQueryCount(context.Background())
	query := fmt.Sprintf(
		"FOR d IN %s FILTER d._from == \"%s\" AND d._to == \"%s\" LIMIT 1 RETURN d",
		d.edges.Name(),
		fmt.Sprintf("%s/%s", d.vertices.Name(), srcID),
		fmt.Sprintf("%s/%s", d.vertices.Name(), dstID))
	cursor, err := d.edges.Database().Query(ctx, query, nil)
	if err != nil {
		return false, ArangoError(err)
	}
	defer cursor.Close()
	if cursor.Count() < 1 {
		return false, nil
	}
	return true, nil
}

func (d *DAG) IsAncestor(id, ancestorId string) (bool, error) {
	ctx := driver.WithQueryCount(context.Background())
	query := fmt.Sprintf("FOR v IN 1..3 INBOUND '%s' GRAPH '%s' FILTER v._id == '%s' RETURN v._key",
		fmt.Sprintf("%s/%s", d.vertices.Name(), id),
		d.graph.Name(),
		fmt.Sprintf("%s/%s", d.vertices.Name(), ancestorId))
	cursor, err := d.edges.Database().Query(ctx, query, nil)
	if err != nil {
		return false, ArangoError(err)
	}
	defer cursor.Close()
	if cursor.Count() >= 1 {
		return true, nil
	}
	return false, nil
}

// EnsureEdge returns returns an error, if srcID or dstID are empty, unknown, or
// the same. EnsureEdge also returns an error, if an edge between srcID and dstID
// does not exist.
func (d *DAG) EnsureEdge(srcID, dstID string) error {

	isEdge, err := d.IsEdge(srcID, dstID)
	if err != nil {
		return err
	}
	if !isEdge {
		return UnknownEdgeError(srcID, dstID)
	}
	return nil
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG) GetOrder() (uint64, error) {
	count, err := d.vertices.Count(context.Background())
	if err != nil {
		return 0, ArangoError(err)
	}
	return uint64(count), nil

}

// GetSize returns the number of edges in the graph.
func (d *DAG) GetSize() (uint64, error) {
	count, err := d.edges.Count(context.Background())
	if err != nil {
		return 0, ArangoError(err)
	}
	return uint64(count), nil
}

/*
func (d *DAG) GetLeaves() (map[string]struct{}, error) {
	// TODO: use bind variables
	query := fmt.Sprintf("FOR d IN %s RETURN d", d.vertices.Name())
	db := d.vertices.Database()
	cursor, err := db.Query(nil, query, nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	leaves := make(map[string]struct{})
	var i map[string]interface{}
	for {
		meta, err := cursor.ReadDocument(nil, &i)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, err
		}

		childCount, err := d.getChildCount(meta.ID)
		if err != nil {
			return nil, err
		}
		if childCount == 0 {
			leaves[meta.Key] = struct{}{}
		}
	}
	return leaves, nil
}

func (d *DAG) getChildCount(id driver.DocumentID) (uint64, error) {
	// TODO: use bind variables
	ctx := driver.WithQueryCount(context.Background())
	query := fmt.Sprintf("FOR d IN %s FILTER d._from == %s RETURN d", d.edges.Name(), id)
	db := d.edges.Database()
	cursor, err := db.Query(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()
	return uint64(cursor.Count()), nil
}

func (d *DAG) GetRoots() (map[string]struct{}, error) {
	// TODO: use bind variables
	query := fmt.Sprintf("FOR d IN %s RETURN d", d.vertices.Name())
	db := d.vertices.Database()
	cursor, err := db.Query(nil, query, nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	leaves := make(map[string]struct{})
	var i map[string]interface{}
	for {
		meta, err := cursor.ReadDocument(nil, &i)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, err
		}

		childCount, err := d.getParentCount(meta.ID)
		if err != nil {
			return nil, err
		}
		if childCount == 0 {
			leaves[meta.Key] = struct{}{}
		}
	}
	return leaves, nil
}

func (d *DAG) getParentCount(id driver.DocumentID) (uint64, error) {
	// TODO: use bind variables
	ctx := driver.WithQueryCount(context.Background())
	query := fmt.Sprintf("FOR d IN %s FILTER d._to == %s RETURN d", d.edges.Name(), id)
	db := d.edges.Database()
	cursor, err := db.Query(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()
	return uint64(cursor.Count()), nil
}

func (d *DAG) GetVertices() (map[string]struct{}, error) {
	// TODO: implement paging
	query := fmt.Sprintf("FOR d IN %s RETURN d", d.vertices.Name())
	db := d.vertices.Database()
	cursor, err := db.Query(nil, query, nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	vertices := make(map[string]struct{})
	var i map[string]interface{}
	for {
		meta, err := cursor.ReadDocument(nil, &i)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, err
		}
		vertices[meta.Key] = struct{}{}
	}
	return vertices, nil
}



/*
func (d *DAG) DeleteVertex(key string) error {
	panic("implement me")
}

func (d *DAG) AddEdge(srcKey, dstKey string) error {
	panic("implement me")
}

func (d *DAG) EnsureEdge(srcKey, dstKey string) (bool, error) {
	panic("implement me")
}

func (d *DAG) DeleteEdge(srcKey, dstKey string) error {
	panic("implement me")
}

func (d *DAG) GetParents(key string) (map[string]struct{}, error) {
	panic("implement me")
}

func (d *DAG) GetChildren(key string) (map[string]struct{}, error) {
	panic("implement me")
}

func (d *DAG) GetAncestors(key string) (map[string]struct{}, error) {
	panic("implement me")
}

func (d *DAG) GetOrderedAncestors(key string) ([]string, error) {
	panic("implement me")
}

func (d *DAG) AncestorsWalker(key string) (chan string, chan bool, error) {
	panic("implement me")
}

func (d *DAG) GetDescendants(key string) (map[string]struct{}, error) {
	panic("implement me")
}

func (d *DAG) GetOrderedDescendants(key string) ([]string, error) {
	panic("implement me")
}

func (d *DAG) DescendantsWalker(v string) (chan string, chan bool, error) {
	panic("implement me")
}

func (d *DAG) String() string {
	panic("implement me")
}
*/
