package arangodag

import (
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
)

const (
	maxDepth = 10000
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

type myKey struct {
	Key string `json:"_key,omitempty"`
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

// GetVertices provides for retrieving the keys of (all) vertices of the DAG.
//
// GetVertices does not directly return the keys but instead returns 3 channels.
// The keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true
// into it.
func (d *DAG) GetVertices() (<-chan string, <-chan error, chan<- bool) {
	query := "FOR v IN @@vertexCollection RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
	}
	return d.walker(query, bindVars)
}

// GetLeaves provides for retrieving the keys of (all) leaves of the DAG.
//
// GetLeaves does not directly return the keys but instead returns 3 channels.
// The keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true
// into it.
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

// GetRoots provides for retrieving the keys of (all) roots of the DAG.
//
// GetRoots does not directly return the keys but instead returns 3 channels. The
// keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true into
// it.
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

// AddEdge adds an edge from the vertex with the key srcKey to the vertex with
// the key dstKey.
//
// AddEdge prevents duplicate edges and loops (and thereby maintains a valid
// DAG).
func (d *DAG) AddEdge(srcKey, dstKey string) error {

	// ensure vertices exist
	srcId, errSrc := d.getVertexId(srcKey)
	if errSrc != nil {
		return errSrc
	}
	dstId, errDst := d.getVertexId(dstKey)
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

// EdgeExists returns true, if an edge between the vertex with the key srcKey and the
// vertex with the key dstKey exists.
func (d *DAG) EdgeExists(srcKey, dstKey string) (bool, error) {
	srcId, errSrc := d.getVertexId(srcKey)
	if errSrc != nil {
		return false, errSrc
	}
	dstId, errDst := d.getVertexId(dstKey)
	if errDst != nil {
		return false, errDst
	}
	return d.edgeExists(srcId, dstId)
}

// GetSize returns the number of edges in the DAG.
func (d *DAG) GetSize() (uint64, error) {
	count, err := d.edges.Count(context.Background())
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}

// GetShortestPath returns the keys of the vertices on the shortest path between
// the vertex with the key srcKey and the vertex with the key dstKey.
//
// GetShortestPath does not directly return the keys but instead returns 3 channels. The
// keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true into
// it.
//
// GetShortestPath "immediately" closes the string-channel, if there is
// no such path.
func (d *DAG) GetShortestPath(srcKey, dstKey string) (<-chan string, <-chan error, chan<- bool, error) {
	srcId, errSrc := d.getVertexId(srcKey)
	if errSrc != nil {
		return nil, nil, nil, errSrc
	}
	dstId, errDst := d.getVertexId(dstKey)
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

// GetParents returns the keys of all parent vertices of the vertex with the key srcKey.
//
// GetParents does not directly return the keys but instead returns 3 channels.
// The keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true into
// it.
//
// GetParents "immediately" closes the string-channel, if there is no parents.
func (d *DAG) GetParents(srcKey string) (<-chan string, <-chan error, chan<- bool, error) {
	return d.getRelatives(srcKey, false, 1, false)
}

// GetAncestors returns the keys of all ancestor vertices of the vertex with the key srcKey in
// breadth first order. If dfs is set to true, the traversal will be executed
// depth-first.
//
// GetAncestors does not directly return the keys but instead returns 3 channels. The
// keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true into
// it.
//
// GetAncestors "immediately" closes the string-channel, if there is
// no ancestors.
func (d *DAG) GetAncestors(srcKey string, dfs bool) (<-chan string, <-chan error, chan<- bool, error) {
	return d.getRelatives(srcKey, false, maxDepth, dfs)
}

// GetDescendants returns the keys of all descendant vertices of the vertex with the key srcKey in
// breadth first order. If dfs is set to true, the traversal will be executed
// depth-first.
//
// GetDescendants does not directly return the keys but instead returns 3 channels. The
// keys may be received from the string-channel. Any errors that occur while
// querying may be received from the error-channel. And, finally, the
// bool-channel may be used to prevent further DB-querying by sending true into
// it.
//
// GetDescendants "immediately" closes the string-channel, if there is
// no ancestors.
func (d *DAG) GetDescendants(srcKey string, dfs bool) (<-chan string, <-chan error, chan<- bool, error) {
	return d.getRelatives(srcKey, true, maxDepth, dfs)
}

func (d *DAG) getRelatives(srcKey string, outbound bool, depth int, dfs bool) (<-chan string, <-chan error, chan<- bool, error) {

	// get the id of the vertex
	id, errVertex := d.getVertexId(srcKey)
	if errVertex != nil {
		return nil, nil, nil, errVertex
	}

	// compute query options / parameters
	uniqueVertices := "global"
	order := "bfs"
	if dfs {
		order = "dfs"
		uniqueVertices = "none"
	}
	direction := "INBOUND"
	if outbound {
		direction = "OUTBOUND"
	}

	// compute the query
	// (somehow INBOUND/OUTBOUND can't be set via bindVars)
	query := fmt.Sprintf("FOR v IN 1..@depth %s @from @@collection " +
		"OPTIONS {order: @order, uniqueVertices: @uniqueVertices} RETURN DISTINCT v", direction)
	bindVars := map[string]interface{}{
		"@collection":    d.edges.Name(),
		"from":           id,
		"order":          order,
		"uniqueVertices": uniqueVertices,
		"depth":          depth,
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

func (d *DAG) DeleteVertex(key string) error {
	panic("implement me")
}

func (d *DAG) DeleteEdge(srcKey, dstKey string) error {
	panic("implement me")
}

func (d *DAG) GetChildren(key string) (map[string]struct{}, error) {
	panic("implement me")
}

*/
