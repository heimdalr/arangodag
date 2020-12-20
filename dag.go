package arangodag

import (
	"context"
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

	return &DAG{vertices: vertices, edges: edges, client: client}, nil
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
		if driver.IsArangoErrorWithErrorNum(err, 1210) {
			return "", DuplicateIDError(id)
		}
		if driver.IsArangoError(err) {
			return "", Error{
				IsDAGError:   true,
				ErrorNum:     ErrArango,
				ErrorMessage: "",
				Err:          err,
			}
		}
		return "", err
	}
	return meta.Key, nil
}

// GetVertex returns the vertex with the given id. GetVertex returns an error, if
// id is empty or unknown.
func (d *DAG) GetVertex(id string, vertex interface{}) error {
	if id == "" {
		return EmptyIDError()
	}

	ctx := context.Background()
	doc := arangoDocContainer{Payload: vertex}
	_, err := d.vertices.ReadDocument(ctx, id, &doc)
	if err != nil {
		if driver.IsArangoErrorWithErrorNum(err, 1202) {
			return NewUnknownKeyError(id)
		}
		if driver.IsArangoError(err) {
			return Error{
				IsDAGError:   true,
				ErrorNum:     ErrArango,
				ErrorMessage: "",
				Err:          err,
			}
		}
		return err
	}
	//vertex = doc.Payload
	return nil
}

/*
func (d *DAG) GetOrder() (uint64, error) {
	count, err := d.vertices.Count(nil)
	if err != nil {
		return 0, err
	}
	return uint64(count), nil

}

func (d *DAG) GetSize() (uint64, error) {
	count, err := d.edges.Count(nil)
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}

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

func (d *DAG) IsEdge(srcKey, dstKey string) (bool, error) {
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
