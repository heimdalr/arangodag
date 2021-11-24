package arangodag

import (
	"context"
	"github.com/arangodb/go-driver"
	"github.com/emicklei/dot"
)

// Dump the graph to dot notation.
func (d *DAG) Dump() (result string, err error) {

	// initialize dot graph
	g := dot.NewGraph(dot.Directed)

	// mapping between arangoDB-vertex keys and dot nodes
	keyNodes := make(map[string]dot.Node)

	// read all vertices
	cursorVert, errVert := d.GetVertices()
	if errVert != nil {
		return "", errVert
	}
	defer func() {
		errCursor := cursorVert.Close()
		if errCursor != nil {
			err = errCursor
		}
	}()
	ctx := context.Background()
	var vertex driver.DocumentMeta
	for {
		_, errRead := cursorVert.ReadDocument(ctx, &vertex)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			return "", errRead
		}
		keyNodes[vertex.ID.String()] = g.Node(vertex.Key)
	}

	// read all vertices
	cursorEdges, errEdges := d.GetEdges()
	if errEdges != nil {
		return "", errEdges
	}
	defer func() {
		errCursor := cursorEdges.Close()
		if errCursor != nil {
			err = errCursor
		}
	}()
	var edge struct {
		From string `json:"_from"`
		To   string `json:"_to"`
	}
	for {
		_, errRead := cursorEdges.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			return "", errRead
		}
		g.Edge(keyNodes[edge.From], keyNodes[edge.To])
	}

	return g.String(), nil
}