package arangodag_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/go-test/deep"
	"github.com/heimdalr/arangodag"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type foobar struct {
	A string
	B string
}

type foobarKey struct {
	A   string
	B   string
	Key string `json:"_key"`
}

func TestNewDAG(t *testing.T) {
	t.Parallel()
	someNewDag(t)
}

func TestDAG_AddVertex(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)

	// simple vertex
	autoId, errAdd1 := d.AddVertex(struct{ foo string }{foo: "1"})
	if errAdd1 != nil {
		t.Error(errAdd1)
	}
	if autoId == "" {
		t.Errorf("got: %v, want id", autoId)
	}

	// vertex with id
	id := "1"
	idReturned, errAdd2 := d.AddVertex(idVertex{Key: "1"})
	if errAdd2 != nil {
		t.Error(errAdd2)
	}
	if idReturned != id {
		t.Errorf("got '%s', want %s", idReturned, id)
	}

	// duplicate
	_, errDuplicate := d.AddVertex(idVertex{Key: "1"})
	if errDuplicate == nil {
		t.Errorf("got 'nil', want duplicate Error")
	}

	// nil
	_, errNil := d.AddVertex(nil)
	if errNil == nil {
		t.Errorf("got 'nil',want nil Error")
	}
}

func TestDAG_GetVertex(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)

	v0 := idVertex{Key: "1"}
	k, _ := d.AddVertex(v0)
	var v1 idVertex
	errVert1 := d.GetVertex(k, &v1)
	if errVert1 != nil {
		t.Error(errVert1)
	}
	if deep.Equal(v0, v1) != nil {
		t.Errorf("got %v, want %v", v1, v0)
	}

	// "complex" document without key
	v2 := foobar{A: "foo", B: "bar"}
	var v3 foobar
	k2, _ := d.AddVertex(v2)
	errVert2 := d.GetVertex(k2, &v3)
	if errVert2 != nil {
		t.Error(errVert2)
	}
	if deep.Equal(v2, v3) != nil {
		t.Errorf("got %v, want %v", v3, v2)
	}

	// "complex" document with key
	v4 := foobarKey{A: "foo", B: "bar", Key: "myFancyKey"}
	var v5 foobarKey
	k4, _ := d.AddVertex(v4)
	errVert3 := d.GetVertex(k4, &v5)
	if errVert3 != nil {
		t.Error(errVert3)
	}
	if deep.Equal(v4, v5) != nil {
		t.Errorf("got %v, want %v", v5, v4)
	}

	// unknown
	var v idVertex
	errUnknown := d.GetVertex("foo", v)
	if errUnknown == nil {
		t.Errorf("got 'nil', want document not found")
	}

	// empty
	errEmpty := d.GetVertex("", v)
	if errEmpty == nil {
		t.Errorf("got 'nil', want key is empty")
	}
}

func TestDAG_GetOrder(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	order, err := d.GetOrder()
	if err != nil {
		t.Error(err)
	}
	if order != 0 {
		t.Errorf("got %d, want %d", order, 0)
	}

	for i := 1; i <= 10; i++ {
		_, _ = d.AddVertex(idVertex{Key: strconv.Itoa(i)})
		order, err = d.GetOrder()
		if err != nil {
			t.Error(err)
		}
		if int(order) != i {
			t.Errorf("got %d, want %d", order, 1)
		}
	}
}


func TestDAG_GetVertices(t *testing.T) {
	t.Parallel()
	tests := []struct {
		d       *arangodag.DAG
		name    string
		prepare func(d *arangodag.DAG)
		want    []string
	}{
		{
			d:       someNewDag(t),
			name:    "no vertex",
			prepare: func(d *arangodag.DAG) {},
			want:    nil,
		},
		{
			d:    someNewDag(t),
			name: "single vertex",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "two vertices",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
			},
			want: []string{"0", "1"},
		},
		{
			d:    someNewDag(t),
			name: "10 vertices",
			prepare: func(d *arangodag.DAG) {
				for i := 0; i < 10; i++ {
					dstKey := strconv.Itoa(i)
					_, _ = d.AddVertex(idVertex{dstKey})
				}
			},
			want: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetVertices()
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetLeaves(t *testing.T) {
	t.Parallel()
	tests := []struct {
		d       *arangodag.DAG
		name    string
		prepare func(d *arangodag.DAG)
		want    []string
	}{
		{
			d:       someNewDag(t),
			name:    "no vertex",
			prepare: func(d *arangodag.DAG) {},
			want:    nil,
		},
		{
			d:    someNewDag(t),
			name: "single vertex",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "one \"real\" leave",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
				_ = d.AddEdge("0", "1")
			},
			want: []string{"1"},
		},
		{
			d:    someNewDag(t),
			name: "10 leaves",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				for i := 1; i < 10; i++ {
					dstKey := strconv.Itoa(i)
					_, _ = d.AddVertex(idVertex{dstKey})
					_ = d.AddEdge("0", dstKey)
				}
			},
			want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetLeaves()
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetRoots(t *testing.T) {
	t.Parallel()
	tests := []struct {
		d       *arangodag.DAG
		name    string
		prepare func(d *arangodag.DAG)
		want    []string
	}{
		{
			d:       someNewDag(t),
			name:    "no vertex",
			prepare: func(d *arangodag.DAG) {},
			want:    nil,
		},
		{
			d:    someNewDag(t),
			name: "single vertex",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "one \"real\" root",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
				_ = d.AddEdge("0", "1")
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "10 leaves",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
				_ = d.AddEdge("0", "1")
				for i := 2; i < 10; i++ {
					srcKey := strconv.Itoa(i)
					_, _ = d.AddVertex(idVertex{srcKey})
					_ = d.AddEdge(srcKey, "1")
				}
			},
			want: []string{"0", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetRoots()
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_AddEdge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		d       *arangodag.DAG
		name    string
		prepare func(d *arangodag.DAG)
		want    error
		srcKey  string
		dstKey  string
	}{
		{
			d:    someNewDag(t),
			name: "happy path",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
			},
			want:   nil,
			srcKey: "0",
			dstKey: "1",
		},
		{
			d:    someNewDag(t),
			name: "unknown Vertex",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
			},
			want: driver.ArangoError{
				HasError:     true,
				Code:         404,
				ErrorNum:     1202,
				ErrorMessage: "document not found",
			},
			srcKey: "0",
			dstKey: "2",
		},
		{
			d:    someNewDag(t),
			name: "duplicate edge",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
				err := d.AddEdge("0", "1")
				if err != nil {
					t.Fatal(err)
				}
			},
			want:   errors.New("duplicate edge"),
			srcKey: "0",
			dstKey: "1",
		},
		{
			d:    someNewDag(t),
			name: "loop",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(idVertex{"0"})
				_, _ = d.AddVertex(idVertex{"1"})
				err := d.AddEdge("0", "1")
				if err != nil {
					t.Fatal(err)
				}
			},
			want:   errors.New("loop"),
			srcKey: "1",
			dstKey: "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			got := tt.d.AddEdge(tt.srcKey, tt.dstKey)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_EdgeExists(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	k1, _ := d.AddVertex(idVertex{"0"})
	k2, _ := d.AddVertex(idVertex{"1"})

	// before adding the edge
	isEdge, errIsEdge := d.EdgeExists(k1, k2)
	if errIsEdge != nil {
		t.Error(errIsEdge)
	}
	if isEdge {
		t.Errorf("EdgeExists() = %t, want %t", isEdge, false)
	}

	// adding edge
	errAddEdge := d.AddEdge(k1, k2)
	if errAddEdge != nil {
		t.Error(errAddEdge)
	}

	// after adding edge
	isEdge2, errIsEdge2 := d.EdgeExists(k1, k2)
	if errIsEdge2 != nil {
		t.Error(errIsEdge2)
	}
	if !isEdge2 {
		t.Errorf("EdgeExists() = %t, want %t", isEdge2, true)
	}
}

func TestDAG_GetSize(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	got, err := d.GetSize()
	if err != nil {
		t.Error(err)
	}
	if got != 0 {
		t.Errorf("got %d, want %d", got, 0)
	}

	for i := 1; i <= 9; i++ {
		id1, _ := d.AddVertex(idVertex{strconv.Itoa(i * 10)})
		id2, _ := d.AddVertex(idVertex{strconv.Itoa(i*10 + 1)})
		_ = d.AddEdge(id1, id2)
		got, err = d.GetSize()
		if err != nil {
			t.Error(err)
		}
		if int(got) != i {
			t.Errorf("got %d, want %d", got, 1)
		}
	}
}

func TestDAG_GetShortestPath(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	tests := []struct {
		name   string
		want   []string
		srcKey string
		dstKey string
	}{
		{
			name:   "happy path",
			want:   []string{"2", "3", "4"},
			srcKey: "2",
			dstKey: "4",
		},
		{
			name:   "path doesn't exist",
			want:   nil,
			srcKey: "0",
			dstKey: "5",
		},
		{
			name:   "shortest path",
			want:   []string{"1", "4"},
			srcKey: "1",
			dstKey: "4",
		},
		{
			name:   "two shortest paths, pick BFS",
			want:   []string{"0", "1", "4"},
			srcKey: "0",
			dstKey: "4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetShortestPath(tt.srcKey, tt.dstKey)
			got := collector(t, cursor, err)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetParents(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	tests := []struct {
		name   string
		want   []string
		srcKey string
	}{
		{
			name:   "no parents",
			want:   nil,
			srcKey: "0",
		},
		{
			name:   "one parent",
			want:   []string{"1"},
			srcKey: "2",
		},
		{
			name:   "two parents",
			want:   []string{"0", "2"},
			srcKey: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetParents(tt.srcKey)
			got := collector(t, cursor, err)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetAncestors(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	tests := []struct {
		name   string
		want   []string
		srcKey string
		dfs    bool
	}{
		{
			name:   "no ancestors",
			want:   nil,
			srcKey: "0",
			dfs:    false,
		},
		{
			name:   "one ancestors",
			want:   []string{"0"},
			srcKey: "1",
			dfs:    false,
		},
		{
			name:   "simple chain",
			want:   []string{"1", "0"},
			srcKey: "2",
			dfs:    false,
		},
		{
			name:   "several parents",
			want:   []string{"0", "2", "1"},
			srcKey: "3",
			dfs:    false,
		},
		{
			name:   "several parents BFS",
			want:   []string{"3", "1", "0", "2"},
			srcKey: "4",
			dfs:    false,
		},
		{
			name:   "several parents DFS",
			want:   []string{"1", "0", "3", "2"},
			srcKey: "4",
			dfs:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetAncestors(tt.srcKey, tt.dfs)
			got := collector(t, cursor, err)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetDescendants(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	tests := []struct {
		name   string
		want   []string
		srcKey string
		dfs    bool
	}{
		{
			name:   "no descendants",
			want:   nil,
			srcKey: "4",
			dfs:    false,
		},
		{
			name:   "one descendant",
			want:   []string{"4"},
			srcKey: "3",
			dfs:    false,
		},
		{
			name:   "simple chain",
			want:   []string{"3", "4"},
			srcKey: "2",
			dfs:    false,
		},
		{
			name:   "several descendants",
			want:   []string{"4", "2", "3"},
			srcKey: "1",
			dfs:    false,
		},
		{
			name:   "several descendants BFS",
			want:   []string{"3", "1", "2", "4"},
			srcKey: "0",
			dfs:    false,
		},
		{
			name:   "several descendants DFS",
			want:   []string{"1", "2", "3", "4"},
			srcKey: "0",
			dfs:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetDescendants(tt.srcKey, tt.dfs)
			got := collector(t, cursor, err)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func collector(t *testing.T, cursor driver.Cursor, errFn error) []string {
	if errFn != nil {
		t.Fatal(errFn)
	}
	defer func() {
		errClose := cursor.Close()
		if errClose != nil {
			t.Error(errClose)
		}
	}()
	ctx := context.Background()
	var vertex driver.DocumentMeta
	var collect []string
	for {
		_, errRead := cursor.ReadDocument(ctx, &vertex)
		if driver.IsNoMoreDocuments(errRead) {
			break
		}
		if errRead != nil {
			t.Fatal(errRead)
		}
		collect = append(collect, vertex.Key)
	}
	return collect
}


func standardDAG(t *testing.T) *arangodag.DAG {

	/*
	     0   5
	    /|
	   | 1
	   | |\
	   | 2 |
	    \| |
	     3 |
	     |/
	     4
	*/

	d := someNewDag(t)
	_, _ = d.AddVertex(idVertex{"0"})
	_, _ = d.AddVertex(idVertex{"1"})
	_, _ = d.AddVertex(idVertex{"2"})
	_, _ = d.AddVertex(idVertex{"3"})
	_, _ = d.AddVertex(idVertex{"4"})
	_, _ = d.AddVertex(idVertex{"5"})
	_ = d.AddEdge("0", "1")
	_ = d.AddEdge("1", "2")
	_ = d.AddEdge("1", "4")
	_ = d.AddEdge("2", "3")
	_ = d.AddEdge("3", "4")
	_ = d.AddEdge("0", "3")
	return d
}

func someNewDag(t *testing.T) *arangodag.DAG {

	// get arangoDB host and port from environment
	host := os.Getenv("ARANGODB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("ARANGODB_PORT")
	if port == "" {
		port = "8529"
	}

	// new connection
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{fmt.Sprintf("http://%s:%s", host, port)},
	})
	if err != nil {
		t.Fatalf("failed to setup connection: %v", err)
	}

	// new client
	client, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		t.Fatalf("failed to setup client: %v", err)
	}

	dbName := someName()
	vertexCollName := someName()
	edgeCollName := someName()

	d, err := arangodag.NewDAG(dbName, vertexCollName, edgeCollName, client)
	if err != nil {
		t.Fatalf("failed to setup new dag: %v", err)
	}
	return d
}

func someName() string {
	//return fmt.Sprintf("test_%s", uuid.New().String())
	return fmt.Sprintf("test_%d", time.Now().UnixNano())
}

type idVertex struct {
	Key string `json:"_key"`
}
