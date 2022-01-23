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
	DB       driver.Database
	Vertices driver.Collection
	Edges    driver.Collection
}

type dagEdge struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}

// NewDAG creates / initializes a new DAG.
func NewDAG(ctx context.Context, dbName, collectionName string, client driver.Client) (d *DAG, err error) {

	// use or create database
	var db driver.Database
	var exists bool

	if exists, err = client.DatabaseExists(ctx, dbName); err != nil {
		return
	}
	if exists {
		db, err = client.Database(ctx, dbName)
	} else {
		db, err = client.CreateDatabase(ctx, dbName, nil)
	}
	if err != nil {
		return
	}

	// use or create vertex collection
	var vertices driver.Collection
	vertexCollName := fmt.Sprintf("%s-%s", "v", collectionName)
	if exists, err = db.CollectionExists(ctx, vertexCollName); err != nil {
		return
	}
	if exists {
		vertices, err = db.Collection(ctx, vertexCollName)
	} else {
		vertices, err = db.CreateCollection(ctx, vertexCollName, nil)
	}
	if err != nil {
		return
	}

	// use or create edge collection
	var edges driver.Collection
	edgeCollName := fmt.Sprintf("%s-%s", "e", collectionName)
	if exists, err = db.CollectionExists(ctx, edgeCollName); err != nil {
		return
	}
	if exists {
		edges, err = db.Collection(ctx, edgeCollName)
	} else {
		collectionOptions := &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
		}
		if edges, err = db.CreateCollection(ctx, edgeCollName, collectionOptions); err != nil {
			return
		}

		// ensure unique edges (from->to) (see: https://stackoverflow.com/a/43006762)
		if _, _, err = edges.EnsureHashIndex(
			ctx,
			[]string{"_from", "_to"},
			&driver.EnsureHashIndexOptions{Unique: true},
		); err != nil {
			return
		}
	}
	if err != nil {
		return
	}

	return &DAG{DB: db, Vertices: vertices, Edges: edges}, nil
}

// AddVertex adds the given vertex to the DAG and returns its key.
//
// If the given vertex contains a `_key` field, this will be used as key. A new
// key will be created otherwise.
//
// Use the type json.RawMessage (i.e. []byte) to add "raw" JSON strings /
// byte slices.
//
// AddVertex prevents duplicate keys.
func (d *DAG) AddVertex(ctx context.Context, vertex interface{}) (meta driver.DocumentMeta, err error) {
	return d.Vertices.CreateDocument(driver.WithQueryCount(ctx), vertex)
}

// AddVertices adds the given vertices to the DAG and returns their keys.
//
// For vertices with `_key` field, this will be used as key. A new
// key will be created otherwise.
//
// AddVertices prevents duplicate keys.
func (d *DAG) AddVertices(ctx context.Context, vertices interface{}) (driver.DocumentMetaSlice, driver.ErrorSlice, error) {
	return d.Vertices.CreateDocuments(ctx, vertices)
}

// GetVertex returns the vertex with the key srcKey.
//
// If src doesn't exist, GetVertex returns an error.
func (d *DAG) GetVertex(ctx context.Context, srcKey string, vertex interface{}) (driver.DocumentMeta, error) {
	return d.Vertices.ReadDocument(ctx, srcKey, vertex)
}

// GetVertices returns the vertices with the given keys.
//
// If src doesn't exist, GetVertex returns an error.
func (d *DAG) GetVertices(ctx context.Context, keys []string, vertex interface{}) (driver.DocumentMetaSlice, driver.ErrorSlice, error) {
	return d.Vertices.ReadDocuments(ctx, keys, vertex)
}

// DelVertex removes the vertex with the key srcKey (src). DelVertex also removes
// any inbound and outbound edges. In case of success, DelVertex returns the
// number of edges that were removed.
//
// If src doesn't exist, DelVertex returns an error.
func (d *DAG) DelVertex(ctx context.Context, srcKey string) (count int64, err error) {

	// delete edges
	id := driver.NewDocumentID(d.Vertices.Name(), srcKey)
	query := "FOR e IN @@edgeCollection " +
		"FILTER e._from == @from || e._to == @from " +
		"REMOVE { _key: e._key } IN @@edgeCollection " +
		"RETURN e"
	bindVars := map[string]interface{}{
		"from":            id,
		"@edgeCollection": d.Edges.Name(),
	}
	var cursor driver.Cursor
	if cursor, err = d.DB.Query(driver.WithQueryCount(ctx), query, bindVars); err != nil {
		return
	}
	count = cursor.Count()
	if err = cursor.Close(); err != nil {
		return
	}

	// remove vertex
	_, err = d.Vertices.RemoveDocument(ctx, srcKey)
	return
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG) GetOrder(ctx context.Context) (int64, error) {
	return d.Vertices.Count(ctx)

}

// GetAllVertices executes the query to retrieve all vertices of the DAG.
// GetAllVertices returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetAllVertices(ctx context.Context) (driver.Cursor, error) {
	query := "FOR v IN @@vertexCollection RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.Vertices.Name(),
	}
	return d.DB.Query(ctx, query, bindVars)
}

// GetLeaves executes the query to retrieve all leaves of the DAG.
// GetLeaves returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetLeaves(ctx context.Context) (driver.Cursor, error) {

	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 OUTBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.Vertices.Name(),
		"@edgeCollection":   d.Edges.Name(),
	}
	return d.DB.Query(ctx, query, bindVars)
}

// GetRoots executes the query to retrieve all roots of the DAG.
// GetRoots returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetRoots(ctx context.Context) (driver.Cursor, error) {
	query := "FOR v IN @@vertexCollection " +
		"FILTER LENGTH(FOR vv IN 1..1 INBOUND v @@edgeCollection LIMIT 1 RETURN 1) == 0 " +
		"RETURN v"
	bindVars := map[string]interface{}{
		"@vertexCollection": d.Vertices.Name(),
		"@edgeCollection":   d.Edges.Name(),
	}
	return d.DB.Query(ctx, query, bindVars)
}

// AddEdge adds an edge from the vertex with the key srcKey (src) to the vertex with
// the key dstKey (dst) and returns the key of the new edge.
//
// AddEdge returns an error, if src or dst don't exist.
//
// AddEdge prevents duplicate edges and loops (and thereby maintains a valid
// DAG).
func (d *DAG) AddEdge(ctx context.Context, srcKey, dstKey string) (driver.DocumentMeta, error) {

	// prepare a slice for ReadDocuments results.
	// TODO: for ReadDocument() you can pass in nil; how to avoid body parsing in array context []interface{nil, nil} doesn't work
	results := make([]struct{}, 2)

	// ensure vertices exist
	metaSlice, errorSlice, err := d.Vertices.ReadDocuments(ctx, []string{srcKey, dstKey}, results)
	if err != nil {
		return driver.DocumentMeta{}, err
	}
	for _, err = range errorSlice {
		if err != nil {
			return driver.DocumentMeta{}, err
		}
	}

	return d.addEdge(ctx, metaSlice[0].ID.String(), metaSlice[1].ID.String())
}

/*
// AddEdgeUnchecked adds an edge from the vertex with the key srcKey (src) to the vertex with
// the key dstKey (dst) and returns the key of the new edge.
//
// AddEdgeUnchecked does NOT return an error, if src or dst don't exist.
//
// AddEdgeUnchecked prevents duplicate edges and loops (and thereby maintains a valid
// DAG).
func (d *DAG) AddEdgeUnchecked(ctx context.Context, srcKey, dstKey string) (driver.DocumentMeta, error) {

	srcID := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.Vertices.Name(), dstKey).String()
	return d.addEdge(ctx, srcID, dstID)
}
*/

// DelEdge removes the edge from the vertex with the key srcKey (src) to the vertex with
// the key dstKey (dst).
//
// DelEdge returns an error, if such an edge doesn't exist.
func (d *DAG) DelEdge(ctx context.Context, srcKey, dstKey string) (meta driver.DocumentMeta, err error) {
	if meta, err = d.getEdge(ctx, srcKey, dstKey); err != nil {
		return
	}
	if meta.Key == "" {
		return meta, driver.ArangoError{
			HasError:     true,
			Code:         404,
			ErrorNum:     1202,
			ErrorMessage: "document not found",
		}
	}
	return d.Edges.RemoveDocument(ctx, meta.Key)
}

// EdgeExists returns true, if an edge between the vertex with the key srcKey (src) and
// the vertex with the key dstKey (dst) exists. If src or dst don't
// exist, EdgeExists returns false.
func (d *DAG) EdgeExists(ctx context.Context, srcKey, dstKey string) (bool, error) {
	meta, err := d.getEdge(ctx, srcKey, dstKey)
	if err != nil {
		return false, err
	}
	return meta.Key != "", nil
}

// GetSize returns the number of edges in the DAG.
func (d *DAG) GetSize(ctx context.Context) (int64, error) {
	return d.Edges.Count(ctx)
}

// GetEdges executes the query to retrieve all edges of the DAG. GetEdges returns
// a cursor that may be used retrieve the edges one-by-one.
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetEdges(ctx context.Context) (driver.Cursor, error) {

	query := "FOR v IN @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
	}
	return d.DB.Query(ctx, query, bindVars)
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
func (d *DAG) GetShortestPath(ctx context.Context, srcKey, dstKey string) (driver.Cursor, error) {
	srcID := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.Vertices.Name(), dstKey).String()
	query := "FOR v IN OUTBOUND SHORTEST_PATH @from TO @to @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	return d.DB.Query(ctx, query, bindVars)
}

// GetParents executes the query to retrieve all parents of the vertex with the key
// srcKey (src). GetParents returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetParents(ctx context.Context, srcKey string) (driver.Cursor, error) {
	return d.getRelatives(ctx, srcKey, false, 1, false)
}

// GetParentCount returns the number parent-vertices of the vertex with the key
// srcKey (src).
//
// If src doesn't exist, GetParentCount returns 0.
func (d *DAG) GetParentCount(ctx context.Context, srcKey string) (count int64, err error) {
	srcID := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()
	query := "FOR v IN 1..1 INBOUND @from @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
		"from":        srcID,
	}
	count, err = d.count(ctx, query, bindVars)
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
func (d *DAG) GetAncestors(ctx context.Context, srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(ctx, srcKey, false, maxDepth, dfs)
}

// GetChildren executes the query to retrieve all children of the vertex with the key
// srcKey. GetChildren returns a cursor that may be used retrieve the vertices
// one-by-one.
//
// If src doesn't exist, the cursor doesn't return any vertex (i.e. no error
// is returned).
//
// It is up to the caller to close the cursor, if no longer needed.
func (d *DAG) GetChildren(ctx context.Context, srcKey string) (driver.Cursor, error) {
	return d.getRelatives(ctx, srcKey, true, 1, false)
}

// GetChildCount returns the number child-vertices of the vertex with the key
// srcKey.
//
// If src doesn't exist, GetChildCount returns 0.
func (d *DAG) GetChildCount(ctx context.Context, srcKey string) (count int64, err error) {
	srcID := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()
	query := "FOR v IN 1..1 OUTBOUND @from @@collection RETURN v"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
		"from":        srcID,
	}
	count, err = d.count(ctx, query, bindVars)
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
func (d *DAG) GetDescendants(ctx context.Context, srcKey string, dfs bool) (driver.Cursor, error) {
	return d.getRelatives(ctx, srcKey, true, maxDepth, dfs)
}

// DotGraph returns a (dot-) graph of the DAG.
func (d *DAG) DotGraph(ctx context.Context, g *dot.Graph) (nodeMapping map[string]dot.Node, err error) {

	// mapping between arangoDB-vertex keys and dot nodes
	nodeMapping = make(map[string]dot.Node)

	var cursor driver.Cursor

	// read all vertices
	if cursor, err = d.GetAllVertices(ctx); err != nil {
		return
	}
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
		nodeMapping[vertex.ID.String()] = node
	}
	if err = cursor.Close(); err != nil {
		return
	}

	// read all vertices
	if cursor, err = d.GetEdges(ctx); err != nil {
		return
	}

	var edge dagEdge
	for {
		_, errRead := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			err = errRead
			return
		}
		g.Edge(nodeMapping[edge.From], nodeMapping[edge.To])
	}
	if err = cursor.Close(); err != nil {
		return
	}
	return
}

// String returns a (graphviz) dot representation of the DAG.
func (d *DAG) String(ctx context.Context) (result string, err error) {

	// transform to dot graph
	g := dot.NewGraph(dot.Directed)
	if _, err = d.DotGraph(ctx, g); err != nil {
		return
	}

	// get the dot string
	result = g.String()
	return
}

func (d *DAG) getRelatives(ctx context.Context, srcKey string, outbound bool, depth int, dfs bool) (driver.Cursor, error) {

	id := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()

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
		"@collection":    d.Edges.Name(),
		"from":           id,
		"order":          order,
		"uniqueVertices": uniqueVertices,
		"depth":          depth,
	}

	return d.DB.Query(ctx, query, bindVars)
}
func (d *DAG) addEdge(ctx context.Context, srcID, dstID string) (meta driver.DocumentMeta, err error) {

	// prevent loops
	var pathExists bool
	if pathExists, err = d.pathExists(ctx, dstID, srcID); err != nil {
		return
	}
	if pathExists {
		return meta, errors.New("loop")
	}

	// add edge
	edge := dagEdge{srcID, dstID}
	return d.Edges.CreateDocument(ctx, edge)
}

func (d *DAG) getEdge(ctx context.Context, srcKey, dstKey string) (meta driver.DocumentMeta, err error) {
	srcID := driver.NewDocumentID(d.Vertices.Name(), srcKey).String()
	dstID := driver.NewDocumentID(d.Vertices.Name(), dstKey).String()
	query := "FOR v, e IN 1..1 OUTBOUND @from @@collection FILTER v._id == @to LIMIT 1 RETURN e"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	var cursor driver.Cursor
	if cursor, err = d.DB.Query(ctx, query, bindVars); err != nil {
		return
	}
	meta, err = cursor.ReadDocument(ctx, &struct{}{})
	if driver.IsNoMoreDocuments(err) {
		return meta, nil
	}
	return meta, nil
}

func (d *DAG) pathExists(ctx context.Context, srcID, dstID string) (bool, error) {
	query := "FOR p IN OUTBOUND SHORTEST_PATH @from TO @to @@collection LIMIT 1 RETURN p"
	bindVars := map[string]interface{}{
		"@collection": d.Edges.Name(),
		"from":        srcID,
		"to":          dstID,
	}
	return d.exists(ctx, query, bindVars)
}

func (d *DAG) exists(ctx context.Context, query string, bindVars map[string]interface{}) (exists bool, err error) {
	var count int64
	count, err = d.count(ctx, query, bindVars)
	if err != nil {
		return
	}
	exists = count > 0
	return
}

func (d *DAG) count(ctx context.Context, query string, bindVars map[string]interface{}) (count int64, err error) {
	ctx = driver.WithQueryCount(ctx)
	var cursor driver.Cursor
	cursor, err = d.DB.Query(ctx, query, bindVars)
	if err != nil {
		return
	}
	count = cursor.Count()
	err = cursor.Close()
	return
}
