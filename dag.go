package arangodag

import (
	"context"
	"errors"
	"github.com/arangodb/go-driver"
)

// DAG implements the data structure of the DAG.
type DAG struct {
	db       driver.Database
	vertices driver.Collection
	edges    driver.Collection
	client   driver.Client
}


type myEdge struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}


// NewDAG creates / initializes a new DAG.
func NewDAG(dbName, vertexCollName, edgeCollName string, client driver.Client) (*DAG, error) {

	// use or create database
	var db driver.Database
	exists, err := client.DatabaseExists(context.Background(), dbName)
	if err != nil {
		return nil, err
	}
	if exists {
		db, err = client.Database(context.Background(), dbName)
	} else {
		db, err = client.CreateDatabase(context.Background(), dbName, nil)
	}
	if err != nil {
		return nil, err
	}

	// use or create vertex collection
	var vertices driver.Collection
	exists, err = db.CollectionExists(context.Background(), vertexCollName)
	if err != nil {
		return nil, err
	}
	if exists {
		vertices, err = db.Collection(context.Background(), vertexCollName)
	} else {
		vertices, err = db.CreateCollection(context.Background(), vertexCollName, nil)
	}
	if err != nil {
		return nil, err
	}

	// use or create edge collection
	var edges driver.Collection
	exists, err = db.CollectionExists(context.Background(), edgeCollName)
	if err != nil {
		return nil, err
	}
	if exists {
		edges, err = db.Collection(context.Background(), edgeCollName)
	} else {
		options := &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		edges, err = db.CreateCollection(context.Background(), edgeCollName, options)
	}
	if err != nil {
		return nil, err
	}

	return &DAG{db: db, vertices: vertices, edges: edges, client: client}, nil
}

// AddVertex adds the given vertex to the DAG and returns its key.
//
// If the given vertex contains a `_key` field, this will be used as key. A new
// key will be created otherwise.
//
// AddVertex prevents duplicate keys.
func (d *DAG) AddVertex(vertex interface{}) (string, error) {

	ctx := driver.WithQueryCount(context.Background())
	meta, err := d.vertices.CreateDocument(ctx, vertex)
	if err != nil {
		return "", err
	}
	return meta.Key, nil
}

// GetVertex returns the vertex with the given key.
func (d *DAG) GetVertex(key string, vertex interface{}) error {
	ctx := context.Background()
	_, err := d.vertices.ReadDocument(ctx, key, vertex)
	if err != nil {
		return err
	}
	return nil
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG) GetOrder() (uint64, error) {
	count, err := d.vertices.Count(context.Background())
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}

// GetVertices returns the keys of all vertices of the DAG.
func (d *DAG) GetVertices() (<-chan string, <-chan error, chan<- bool) {
	query := "FOR v IN @@vertexCollection RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
	}
	return d.walker(query, bindVars)
}

// GetLeaves returns the leaves of the DAG.
func (d *DAG) GetLeaves() (<-chan string, <-chan error, chan<- bool) {

	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 OUTBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
		"@edgeCollection":   d.edges.Name(),
	}
	return d.walker(query, bindVars)
}

// GetRoots returns the roots of the DAG.
func (d *DAG) GetRoots() (<-chan string, <-chan error, chan<- bool) {
	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 INBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
		"@edgeCollection":   d.edges.Name(),
	}
	return d.walker(query, bindVars)
}

// AddEdge adds an edge from src to dst.
func (d *DAG) AddEdge(src, dst string) error {

	// ensure vertices exist
	srcId, errSrc := d.getVertexId(src)
	if errSrc != nil {
		return errSrc
	}
	dstId, errDst := d.getVertexId(dst)
	if errDst != nil {
		return errDst
	}

	// prevent duplicate edge
	existsEdge, errEdge :=d.edgeExists(srcId, dstId)
	if errEdge != nil {
		return errEdge
	}
	if existsEdge {
		return errors.New("duplicate edge")
	}

	// prevent loops
	pathExists, errSrc := d.pathExists(dstId, srcId)
	if errSrc != nil {
		return errSrc
	}
	if pathExists {
		return errors.New("loop")
	}

	// add edge
	ctx := context.Background()
	_, err := d.edges.CreateDocument(ctx, myEdge{srcId, dstId})
	if err != nil {
		return err
	}
	return nil
}

// EdgeExists returns true, if an edge from src to dst exists.
func (d *DAG) EdgeExists(src, dst string) (bool, error) {
	srcId, errSrc := d.getVertexId(src)
	if errSrc != nil {
		return false, errSrc
	}
	dstId, errDst := d.getVertexId(dst)
	if errDst != nil {
		return false, errDst
	}
	return d.edgeExists(srcId, dstId)
}

// GetSize returns the number of edges in the graph.
func (d *DAG) GetSize() (uint64, error) {
	count, err := d.edges.Count(context.Background())
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}

type myKey struct {
	Key string `json:"_key,omitempty"`
}

// GetShortestPath returns the shortest path between src and dst. GetShortestPath returns nil if
// there is no such path.
func (d *DAG) GetShortestPath(src, dst string) (<-chan string, <-chan error, chan<- bool, error) {
	srcId, errSrc := d.getVertexId(src)
	if errSrc != nil {
		return nil, nil, nil, errSrc
	}
	dstId, errDst := d.getVertexId(dst)
	if errDst != nil {
		return nil, nil, nil, errDst
	}
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcId,
		"to":          dstId,
	}
	chanKeys, chanErrors, chanSignal := d.walker(query, bindVars)
	return chanKeys, chanErrors, chanSignal, nil
}

// GetAncestors returns all ancestors of key breadth order. If dfs
// is set to true, the traversal will be executed depth-first.
func (d *DAG) GetAncestors(key string, dfs bool) (<-chan string, <-chan error, chan<- bool, error) {

	// get the id of the vertex
	id, errVertex := d.getVertexId(key)
	if errVertex != nil {
		return nil, nil, nil, errVertex
	}

	// compute query options
	uniqueVertices := "global"
	order := "bfs"
	if dfs {
		order = "dfs"
		uniqueVertices = "none"
	}

	// compute the query
	query := "FOR v IN 1..10000 INBOUND @from @@collection OPTIONS {order: @order, uniqueVertices: @uniqueVertices}" +
		"RETURN DISTINCT v"
	bindVars := map[string]interface{}{
		"@collection":    d.edges.Name(),
		"from":           id,
		"order":          order,
		"uniqueVertices": uniqueVertices,
	}

	chanKeys, chanErrors, chanSignal := d.walker(query, bindVars)
	return chanKeys, chanErrors, chanSignal, nil
}


func (d *DAG) getVertexId(key string) (string, error) {
	ctx := context.Background()
	var data driver.DocumentMeta
	meta, err := d.vertices.ReadDocument(ctx, key, &data)
	if err != nil {
		return "", err
	}
	return string(meta.ID), nil
}

func (d *DAG) edgeExists(srcId, dstId string) (bool, error) {
	query := "FOR v IN 1..1 OUTBOUND @from @@collection FILTER v._id == @to LIMIT 1 RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcId,
		"to":          dstId,
	}
	return d.exists(query, bindVars)
}

func (d *DAG) pathExists(srcId, dstId string) (bool, error) {
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection LIMIT 1 RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcId,
		"to":          dstId,
	}
	return d.exists(query, bindVars)
}

func (d *DAG) exists(query string, bindVars map[string]interface{}) (bool, error) {
	ctx := driver.WithQueryCount(context.Background())
	cursor, err := d.db.Query(ctx, query, bindVars)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Count() > 0, nil
}

func (d *DAG) walker(query string, bindVars map[string]interface{}) (<-chan string, <-chan error, chan<- bool) {

	chanKeys := make(chan string)
	chanErrors := make(chan error)
	chanSignal := make(chan bool, 1)

	go func() {
		defer close(chanErrors)
		defer close(chanKeys)
		ctx := context.Background()
		cursor, err := d.db.Query(ctx, query, bindVars)
		if err != nil {
			chanErrors <- err
			return
		}
		defer func(){
			errClose := cursor.Close()
			if errClose != nil {
				chanErrors <- errClose
			}
		}()

		var key myKey
		for {
			_, errRead := cursor.ReadDocument(ctx, &key)
			if driver.IsNoMoreDocuments(errRead) {
				return
			}
			if errRead != nil {
				chanErrors <- errRead
				continue
			}
			select {
			case <-chanSignal:
				return
			default:
				chanKeys <- key.Key
			}
		}
	}()

	return chanKeys, chanErrors, chanSignal
}

// WalkFunc is the type expected by WalkAncestors.
type WalkFunc func(key string, err error) error


/*
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



func (d *DAG) DeleteVertex(key string) error {
	panic("implement me")
}

func (d *DAG) AddEdge(srcKey, dstKey string) error {
	panic("implement me")
}

func (d *DAG) EdgeExists(srcKey, dstKey string) (bool, error) {
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
