// Package arangodag implements directed acyclic graphs (DAGs) on top of ArangoDB.
package arangodag

import (
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/emicklei/dot"
)

const (
	maxDepth = 10000
)

// DAG implements the data structure of the DAG.
type DAG struct {
	db       driver.Database
	vertices driver.Collection
	edges    driver.Collection
}

// NewDAG creates / initializes a new DAG.
func NewDAG(dbName, collectionName string, client driver.Client) (d *DAG, err error) {

	// use or create database
	var db driver.Database
	var exists bool
	if exists, err = client.DatabaseExists(context.Background(), dbName); err != nil {
		return
	}
	if exists {
		db, err = client.Database(context.Background(), dbName)
	} else {
		db, err = client.CreateDatabase(context.Background(), dbName, nil)
	}
	if err != nil {
		return
	}

	// use or create vertex collection
	var vertices driver.Collection
	vertexCollName := fmt.Sprintf("%s-%s", "v", collectionName)
	if exists, err = db.CollectionExists(context.Background(), vertexCollName); err != nil {
		return
	}
	if exists {
		vertices, err = db.Collection(context.Background(), vertexCollName)
	} else {
		vertices, err = db.CreateCollection(context.Background(), vertexCollName, nil)
	}
	if err != nil {
		return
	}

	// use or create edge collection
	var edges driver.Collection
	edgeCollName := fmt.Sprintf("%s-%s", "e", collectionName)
	if exists, err = db.CollectionExists(context.Background(), edgeCollName); err != nil {
		return
	}
	if exists {
		edges, err = db.Collection(context.Background(), edgeCollName)
	} else {
		collectionOptions := &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
		}
		if edges, err = db.CreateCollection(context.Background(), edgeCollName, collectionOptions); err != nil {
			return
		}

		// ensure unique edges (from->to) (see: https://stackoverflow.com/a/43006762)
		if _, _, err = edges.EnsureHashIndex(
			context.Background(),
			[]string{"_from", "_to"},
			&driver.EnsureHashIndexOptions{Unique: true},
		); err != nil {
			return
		}
	}
	if err != nil {
		return
	}

	return &DAG{db: db, vertices: vertices, edges: edges}, nil
}

// AddVertex adds the given vertex to the DAG and returns its key.
//
// If the given vertex contains a `_key` field, this will be used as key. A new
// key will be created otherwise.
//
// AddVertex prevents duplicate keys.
func (d *DAG) AddVertex(vertex interface{}) (key string, err error) {
	var meta driver.DocumentMeta
	ctx := driver.WithQueryCount(context.Background())
	if meta, err = d.vertices.CreateDocument(ctx, vertex); err != nil {
		return
	}
	return meta.Key, err
}

// GetVertex returns the vertex with the key srcKey.
//
// If src doesn't exist, GetVertex returns an error.
func (d *DAG) GetVertex(srcKey string, vertex interface{}) (err error) {
	ctx := context.Background()
	_, err = d.vertices.ReadDocument(ctx, srcKey, vertex)
	return
}

// DelVertex removes the vertex with the key srcKey (src). DelVertex also removes
// any inbound and outbound edges. In case of success, DelVertex returns the
// number of edges that were removed.
//
// If src doesn't exist, DelVertex returns an error.
func (d *DAG) DelVertex(srcKey string) (edgeCount int64, err error) {

	// delete edges
	id := driver.NewDocumentID(d.vertices.Name(), srcKey)
	query := "FOR e IN @@edgeCollection " +
		"FILTER e._from == @from || e._to == @from " +
		"REMOVE { _key: e._key } IN @@edgeCollection " +
		"RETURN e"
	bindVars := map[string]interface{}{
		"from":            id,
		"@edgeCollection": d.edges.Name(),
	}
	ctx := driver.WithQueryCount(context.Background())
	var cursor driver.Cursor
	if cursor, err = d.db.Query(ctx, query, bindVars); err != nil {
		return
	}
	edgeCount = cursor.Count()
	if err = cursor.Close(); err != nil {
		return
	}

	// remove vertex
	_, err = d.vertices.RemoveDocument(ctx, srcKey)
	return
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG) GetOrder() (count int64, err error) {
	return d.vertices.Count(context.Background())

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

// AddEdge adds an edge from the vertex with the key srcKey (src) to the vertex with
// the key dstKey (dst) and returns the key of the new edge.
//
// AddEdge returns an error, if src or dst don't exist.
//
// AddEdge prevents duplicate edges and loops (and thereby maintains a valid
// DAG).
func (d *DAG) AddEdge(srcKey, dstKey string) (key string, err error) {

	// ensure vertices exist
	var src, dst driver.DocumentMeta
	if err = d.GetVertex(srcKey, &src); err != nil {
		return
	}
	if err = d.GetVertex(dstKey, &dst); err != nil {
		return
	}
	return d.addEdge(src.ID.String(), dst.ID.String())
}

// AddEdgeUnchecked adds an edge from the vertex with the key srcKey (src) to the vertex with
// the key dstKey (dst) and returns the key of the new edge.
//
// AddEdgeUnchecked does NOT return an error, if src or dst don't exist.
//
// AddEdgeUnchecked prevents duplicate edges and loops (and thereby maintains a valid
// DAG).
func (d *DAG) AddEdgeUnchecked(srcKey, dstKey string) (key string, err error) {

	srcID := driver.NewDocumentID(d.vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.vertices.Name(), dstKey).String()
	return d.addEdge(srcID, dstID)
}

func (d *DAG) addEdge(srcID, dstID string) (key string, err error) {

	// prevent loops
	var pathExists bool
	if pathExists, err = d.pathExists(dstID, srcID); err != nil {
		return
	}
	if pathExists {
		return key, errors.New("loop")
	}

	// add edge
	var meta driver.DocumentMeta
	ctx := context.Background()
	edge := struct {
		From string `json:"_from"`
		To   string `json:"_to"`
	}{srcID, dstID}
	if meta, err = d.edges.CreateDocument(ctx, edge); err != nil {
		return
	}
	return meta.Key, nil
}

// EdgeExists returns true, if an edge between the vertex with the key srcKey (src) and
// the vertex with the key dstKey (dst) exists. If src or dst don't
// exist, EdgeExists returns false.
func (d *DAG) EdgeExists(srcKey, dstKey string) (bool, error) {
	srcID := driver.NewDocumentID(d.vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.vertices.Name(), dstKey).String()
	return d.edgeExists(srcID, dstID)
}

// GetSize returns the number of edges in the DAG.
func (d *DAG) GetSize() (int64, error) {
	return d.edges.Count(context.Background())
}

// GetEdges executes the query to retrieve all edges of the DAG. GetEdges returns
// a cursor that may be used retrieve the edges one-by-one.
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
// path between the vertex with the key srcKey (src) and the vertex with the key
// dstKey (dst). GetShortestPath returns a cursor that may be used retrieve the
// vertices one-by-one. The result includes the src and dst.
//
// If src and dst are equal, the cursor will return a single vertex.
//
// If src or dst don't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetShortestPath(srcKey, dstKey string) (driver.Cursor, error) {
	srcID := driver.NewDocumentID(d.vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.vertices.Name(), dstKey).String()
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	ctx := context.Background()
	return d.db.Query(ctx, query, bindVars)
}

// GetParents executes the query to retrieve all parents of the vertex with the key
// srcKey (src). GetParents returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetParents(srcKey string) (driver.Cursor, error) {
	return d.getRelatives(srcKey, false, 1, false)
}

// GetParentCount returns the number parent-vertices of the vertex with the key
// srcKey (src).
//
// If src doesn't exist, GetParentCount returns 0.
func (d *DAG) GetParentCount(srcKey string) (count int64, err error) {
	srcID := driver.NewDocumentID(d.vertices.Name(), srcKey).String()
	query := "FOR v IN 1..1 INBOUND @from @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcID,
	}
	count, err = d.count(query, bindVars)
	if err != nil {
		return
	}
	return
}

// GetAncestors executes the query to retrieve all ancestors of the vertex with the key
// srcKey (src). GetAncestors returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// By default, GetAncestors returns vertices in BFS order, if dfs is set to true,
// it will be in DFS order.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetAncestors(srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(srcKey, false, maxDepth, dfs)
}

// GetChildren executes the query to retrieve all children of the vertex with the key
// srcKey. GetChildren returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetChildren(srcKey string) (driver.Cursor, error) {
	return d.getRelatives(srcKey, true, 1, false)
}

// GetChildCount returns the number child-vertices of the vertex with the key
// srcKey.
//
// If src doesn't exist, GetChildCount returns 0.
func (d *DAG) GetChildCount(srcKey string) (count int64, err error) {
	srcID := driver.NewDocumentID(d.vertices.Name(), srcKey).String()
	query := "FOR v IN 1..1 OUTBOUND @from @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcID,
	}
	count, err = d.count(query, bindVars)
	if err != nil {
		return
	}
	return
}

// GetDescendants executes the query to retrieve all descendants of the vertex with the key
// srcKey. GetDescendants returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// By default, GetDescendants returns vertices in BFS order, if dfs is set to
// true, it will be in DFS order.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetDescendants(srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(srcKey, true, maxDepth, dfs)
}

// DotGraph returns a (dot-) graph of the DAG.
func (d *DAG) DotGraph(g *dot.Graph) (err error) {

	// mapping between arangoDB-vertex keys and dot nodes
	keyNodes := make(map[string]dot.Node)

	var cursor driver.Cursor

	// read all vertices
	if cursor, err = d.GetVertices(); err != nil {
		return
	}
	ctx := context.Background()
	var vertex driver.DocumentMeta
	for {
		_, errRead := cursor.ReadDocument(ctx, &vertex)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			err = errRead
			return
		}
		node := g.Node(vertex.Key).Label(vertex.Key)
		keyNodes[vertex.ID.String()] = node
	}
	if err = cursor.Close(); err != nil {
		return
	}

	// read all vertices
	if cursor, err = d.GetEdges(); err != nil {
		return
	}
	var edge struct {
		From string `json:"_from"`
		To   string `json:"_to"`
	}
	for {
		_, errRead := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			err = errRead
			return
		}
		g.Edge(keyNodes[edge.From], keyNodes[edge.To])
	}
	if err = cursor.Close(); err != nil {
		return
	}
	return
}

// String returns a (graphviz) dot representation of the DAG.
func (d *DAG) String() (result string, err error) {

	// transform to dot graph
	g := dot.NewGraph(dot.Directed)
	if err = d.DotGraph(g); err != nil {
		return
	}

	// get the dot string
	result = g.String()
	return
}

func (d *DAG) getRelatives(srcKey string, outbound bool, depth int, dfs bool) (driver.Cursor, error) {

	id := driver.NewDocumentID(d.vertices.Name(), srcKey).String()

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

func (d *DAG) edgeExists(srcID, dstID string) (bool, error) {
	query := "FOR v IN 1..1 OUTBOUND @from @@collection FILTER v._id == @to LIMIT 1 RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	return d.exists(query, bindVars)
}

func (d *DAG) pathExists(srcID, dstID string) (bool, error) {
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection LIMIT 1 RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	return d.exists(query, bindVars)
}

func (d *DAG) exists(query string, bindVars map[string]interface{}) (exists bool, err error) {
	var count int64
	count, err = d.count(query, bindVars)
	if err != nil {
		return
	}
	exists = count > 0
	return
}

func (d *DAG) count(query string, bindVars map[string]interface{}) (count int64, err error) {
	ctx := driver.WithQueryCount(context.Background())
	var cursor driver.Cursor
	cursor, err = d.db.Query(ctx, query, bindVars)
	if err != nil {
		return
	}
	count = cursor.Count()
	err = cursor.Close()
	return
}
