package arangodag

/*

package dag

// IDInterface describes the interface a type must implement in order to
// explicitly specify vertex keys.
//
// Objects of types not implementing this interface will receive automatically
// generated keys (as of adding them to the graph).
type IDInterface interface {
	ID() string
}

// DAG describes the interface a DAG driver must implement.
type DAG interface {

	// AddVertex adds the given vertex to the DAG and returns its key.
	// AddVertex returns an error, if the vertex is nil. If the vertex
	// implements the IDInterface, the key will be taken from the vertex
	// (itself). In this case, AddVertex returns an error, if the computed
	// key is empty or already exists.
	AddVertex(vertex interface{}) (string, error)

	// GetVertex returns the vertex with the given key. GetVertex returns
	// an error, if the key is empty or unknown.
	GetVertex(key string, vertex interface{}) error

	// DeleteVertex deletes the vertex with the given key. DeleteVertex also
	// deletes all attached edges (inbound and outbound). DeleteVertex returns
	// an error, if key is empty or unknown.
	DeleteVertex(key string) error

	// AddEdge adds an edge between srcKey and dstKey. AddEdge returns an
	// error, if srcKey or dstKey are empty strings or unknown, if the edge
	// already exists, or if the new edge would create a loop.
	AddEdge(srcKey, dstKey string) error

	// IsEdge returns true, if there exists an edge between srcKey and dstKey.
	// IsEdge returns false, if there is no such edge. IsEdge returns an error,
	// if srcKey or dstKey are empty, unknown, or the same.
	IsEdge(srcKey, dstKey string) (bool, error)

	// DeleteEdge deletes the edge between srcKey and dstKey. DeleteEdge
	// returns an error, if srcKey or dstKey are empty or unknown, or if,
	// there is no edge between srcKey and dstKey.
	DeleteEdge(srcKey, dstKey string) error

	// GetOrder returns the number of vertices in the graph.
	GetOrder() (uint64, error)

	// GetSize returns the number of edges in the graph.
	GetSize() (uint64, error)

	// GetLeaves returns the keys of all vertices without children.
	GetLeaves() (map[string]struct{}, error)

	// GetRoots returns the keys of all vertices without parents.
	GetRoots() (map[string]struct{}, error)

	// GetVertices returns the keys of all vertices.
	GetVertices() (map[string]struct{}, error)

	// GetParents returns the keys of all parents of the vertex with the key
	// key. GetParents returns an error, if key is empty or unknown.
	GetParents(key string) (map[string]struct{}, error)

	// GetChildren returns the keys of all children of the vertex with the key
	// key. GetChildren returns an error, if key is empty or unknown.
	GetChildren(key string) (map[string]struct{}, error)

	// GetAncestors return all ancestors of the vertex with the key key.
	// GetAncestors returns an error, if key is empty or unknown.
	GetAncestors(key string) (map[string]struct{}, error)

	// GetOrderedAncestors return all ancestors of the vertex with the key
	// key in a breath-first order. Only the first occurrence of each vertex
	// (i.e. its key) is returned. GetOrderedAncestors returns an error, if
	// key is empty or unknown.
	//
	// Note, there is no order between sibling vertices. Two consecutive runs
	// of GetOrderedAncestors may return different results.
	GetOrderedAncestors(key string) ([]string, error)

	// AncestorsWalker returns a channel and subsequently returns / walks all
	// ancestors of the vertex with the key key in a breath first order.
	// The second channel returned, may be used to stop further walking.
	// AncestorsWalker returns an error, if key is empty or unknown.
	//
	// Note, there is no order between sibling vertices. Two consecutive runs
	// of AncestorsWalker may return different results.
	AncestorsWalker(key string) (chan string, chan bool, error)

	// GetDescendants return all ancestors of the vertex with the key key.
	// GetDescendants returns an error, if key is empty or unknown.
	GetDescendants(key string) (map[string]struct{}, error)

	// GetOrderedDescendants returns all descendants of the vertex with the
	// key key in a breath-first order. Only the first occurrence of each
	// vertex (i.e. its key) is returned. GetOrderedDescendants returns an
	// error, if key is empty or unknown.
	//
	// Note, there is no order between sibling vertices. Two consecutive runs
	// of GetOrderedDescendants may return different results.
	GetOrderedDescendants(key string) ([]string, error)

	// DescendantsWalker returns a channel and subsequently returns / walks all
	// descendants of the vertex with the key key in a breath first order. The
	// second channel returned may be used to stop further walking.
	// DescendantsWalker returns an error, if key is empty or unknown.
	//
	// Note, there is no order between sibling vertices. Two consecutive runs
	// of DescendantsWalker may return different results.
	DescendantsWalker(v string) (chan string, chan bool, error)

	// String return a textual representation of the graph.
	String() string
}

*/