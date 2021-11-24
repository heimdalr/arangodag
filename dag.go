// Package arangodag implements directed acyclic graphs (DAGs) on top of ArangoDB.
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

// GetVertices executes the query to retrieve all vertices of the DAG.
// GetVertices returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetVertices() (driver.Cursor, error) {
	query := "FOR v IN @@vertexCollection RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
}

// GetLeaves executes the query to retrieve all leaves of the DAG.
// GetLeaves returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetLeaves() (driver.Cursor, error) {

	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 OUTBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
		"@edgeCollection":   d.edges.Name(),
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
}

// GetRoots executes the query to retrieve all roots of the DAG.
// GetRoots returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetRoots() (driver.Cursor, error) {
	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 INBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.vertices.Name(),
		"@edgeCollection":   d.edges.Name(),
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
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
	existsEdge, errEdge := d.edgeExists(srcId, dstId)
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
	edge := struct {
		From string `json:"_from"`
		To   string `json:"_to"`
	}{srcId, dstId}
	_, err := d.edges.CreateDocument(ctx, edge)
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

// GetEdges executes the query to retrieve all edges of the DAG.
// GetEdges returns a cursor that may be used retrieve the edges
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetEdges() (driver.Cursor, error) {
	query := "FOR v IN @@vertexCollection RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.edges.Name(),
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
}

// GetShortestPath executes the query to retrieve the vertices on the shortest
// path between the vertex with the key srcKey and the vertex with the key
// dstKey. GetShortestPath returns a cursor that may be used retrieve the
// vertices one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetShortestPath(srcKey, dstKey string) (driver.Cursor, error) {
	srcId, errSrc := d.getVertexId(srcKey)
	if errSrc != nil {
		return nil, errSrc
	}
	dstId, errDst := d.getVertexId(dstKey)
	if errDst != nil {
		return nil, errDst
	}
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcId,
		"to":          dstId,
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
}

// GetParents executes the query to retrieve all parents of the vertex with the key
// srcKey. GetParents returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetParents(srcKey string) (driver.Cursor, error) {
	return d.getRelatives(srcKey, false, 1, false)
}

// GetAncestors executes the query to retrieve all ancestors of the vertex with the key
// srcKey. GetAncestors returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetAncestors(srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(srcKey, false, maxDepth, dfs)
}

// GetChildren executes the query to retrieve all children of the vertex with the key
// srcKey. GetChildren returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetChildren(srcKey string) (driver.Cursor, error) {
	return d.getRelatives(srcKey, true, 1, false)
}

// GetDescendants executes the query to retrieve all descendants of the vertex with the key
// srcKey. GetDescendants returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetDescendants(srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(srcKey, true, maxDepth, dfs)
}

func (d *DAG) getRelatives(srcKey string, outbound bool, depth int, dfs bool) (driver.Cursor, error) {

	// get the id of the vertex
	id, errVertex := d.getVertexId(srcKey)
	if errVertex != nil {
		return nil, errVertex
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
	query := fmt.Sprintf("FOR v IN 1..@depth %s @from @@collection "+
		"OPTIONS {order: @order, uniqueVertices: @uniqueVertices} RETURN DISTINCT v", direction)
	bindVars := map[string]interface{}{
		"@collection":    d.edges.Name(),
		"from":           id,
		"order":          order,
		"uniqueVertices": uniqueVertices,
		"depth":          depth,
	}

	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
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

*/
