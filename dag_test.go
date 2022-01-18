package arangodag_test

import (
	"context"
	"encoding/json"
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

var (
	documentNotFoundError = driver.ArangoError{
		HasError:     true,
		Code:         404,
		ErrorNum:     1202,
		ErrorMessage: "document not found",
	}
)

func TestNewDAG(t *testing.T) {
	t.Parallel()
	someNewDag(t)
}

func TestDAG_AddVertex(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	ctx := context.Background()

	// simple vertex
	autoMeta, errAdd1 := d.AddVertex(ctx, struct{ foo string }{foo: "1"})
	if errAdd1 != nil {
		t.Error(errAdd1)
	}
	if autoMeta.Key == "" {
		t.Errorf("got: %v, want id", autoMeta.Key)
	}

	// vertex with id
	id := "1"
	metaReturned, errAdd2 := d.AddVertex(ctx, idVertex{Key: "1"})
	if errAdd2 != nil {
		t.Error(errAdd2)
	}
	if metaReturned.Key != id {
		t.Errorf("got '%s', want %s", metaReturned.Key, id)
	}

	// duplicate
	_, errDuplicate := d.AddVertex(ctx, idVertex{Key: "1"})
	if errDuplicate == nil {
		t.Errorf("got 'nil', want duplicate Error")
	}

	// nil
	_, errNil := d.AddVertex(ctx, nil)
	if errNil == nil {
		t.Errorf("got 'nil',want nil Error")
	}

	// vertex from byte array
	id = "foo"
	var vertex json.RawMessage = []byte(fmt.Sprintf(`{"_key": "%s", "blub": "bar"}`, id))
	metaReturned2, errAdd3 := d.AddVertex(ctx, vertex)
	if errAdd3 != nil {
		t.Error(errAdd3)
	}
	if metaReturned2.Key != id {
		t.Errorf("got '%s', want %s", metaReturned.Key, id)
	}

}

func TestDAG_AddVertices(t *testing.T) {
	type args struct {
		vertices []json.RawMessage
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Single Vertex",
			args: args{[]json.RawMessage{[]byte(`{"_key": "1"}`)}},
			want: []string{"1"},
		},
		{
			name: "Two Vertices",
			args: args{[]json.RawMessage{[]byte(`{"_key": "1"}`), []byte(`{"_key": "2"}`)}},
			want: []string{"1", "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := someNewDag(t)
			documentMetaSlice, _, err := d.AddVertices(context.Background(), tt.args.vertices)
			got := documentMetaSlice.Keys()
			if err != nil {
				t.Error(err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddVertices() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetVertex(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	ctx := context.Background()

	v0 := idVertex{Key: "1"}
	meta, _ := d.AddVertex(ctx, v0)
	var v1 idVertex
	_, errVert1 := d.GetVertex(ctx, meta.Key, &v1)
	if errVert1 != nil {
		t.Error(errVert1)
	}
	if deep.Equal(v0, v1) != nil {
		t.Errorf("got %v, want %v", v1, v0)
	}

	// "complex" document without key
	v2 := foobar{A: "foo", B: "bar"}
	var v3 foobar
	meta2, _ := d.AddVertex(ctx, v2)
	_, errVert2 := d.GetVertex(ctx, meta2.Key, &v3)
	if errVert2 != nil {
		t.Error(errVert2)
	}
	if deep.Equal(v2, v3) != nil {
		t.Errorf("got %v, want %v", v3, v2)
	}

	// "complex" document with key
	v4 := foobarKey{A: "foo", B: "bar", Key: "myFancyKey"}
	var v5 foobarKey
	meta4, _ := d.AddVertex(ctx, v4)
	_, errVert3 := d.GetVertex(ctx, meta4.Key, &v5)
	if errVert3 != nil {
		t.Error(errVert3)
	}
	if deep.Equal(v4, v5) != nil {
		t.Errorf("got %v, want %v", v5, v4)
	}

	// unknown
	var v idVertex
	_, errUnknown := d.GetVertex(ctx, "foo", v)
	if errUnknown == nil {
		t.Errorf("got 'nil', want document not found")
	}

	// empty
	_, errEmpty := d.GetVertex(ctx, "", v)
	if errEmpty == nil {
		t.Errorf("got 'nil', want key is empty")
	}
}

func TestDAG_GetOrder(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	ctx := context.Background()
	order, err := d.GetOrder(ctx)
	if err != nil {
		t.Error(err)
	}
	if order != 0 {
		t.Errorf("got %d, want %d", order, 0)
	}

	for i := 1; i <= 10; i++ {
		_, _ = d.AddVertex(ctx, idVertex{Key: strconv.Itoa(i)})
		order, err = d.GetOrder(ctx)
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
	ctx := context.Background()
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
				_, _ = d.AddVertex(ctx, idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "two vertices",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(ctx, idVertex{"0"})
				_, _ = d.AddVertex(ctx, idVertex{"1"})
			},
			want: []string{"0", "1"},
		},
		{
			d:    someNewDag(t),
			name: "10 vertices",
			prepare: func(d *arangodag.DAG) {
				for i := 0; i < 10; i++ {
					dstKey := strconv.Itoa(i)
					_, _ = d.AddVertex(ctx, idVertex{dstKey})
				}
			},
			want: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetVertices(ctx)
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetLeaves(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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
				_, _ = d.AddVertex(ctx, idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "one \"real\" leave",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(ctx, idVertex{"0"})
				_, _ = d.AddVertex(ctx, idVertex{"1"})
				_, _ = d.AddEdge(ctx, "0", "1")
			},
			want: []string{"1"},
		},
		{
			d:    someNewDag(t),
			name: "10 leaves",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(ctx, idVertex{"0"})
				for i := 1; i < 10; i++ {
					dstKey := strconv.Itoa(i)
					_, _ = d.AddVertex(ctx, idVertex{dstKey})
					_, _ = d.AddEdge(ctx, "0", dstKey)
				}
			},
			want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetLeaves(ctx)
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetRoots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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
				_, _ = d.AddVertex(ctx, idVertex{"0"})
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "one \"real\" root",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(ctx, idVertex{"0"})
				_, _ = d.AddVertex(ctx, idVertex{"1"})
				_, _ = d.AddEdge(ctx, "0", "1")
			},
			want: []string{"0"},
		},
		{
			d:    someNewDag(t),
			name: "10 leaves",
			prepare: func(d *arangodag.DAG) {
				_, _ = d.AddVertex(ctx, idVertex{"0"})
				_, _ = d.AddVertex(ctx, idVertex{"1"})
				_, _ = d.AddEdge(ctx, "0", "1")
				for i := 2; i < 10; i++ {
					srcKey := strconv.Itoa(i)
					_, _ = d.AddVertex(ctx, idVertex{srcKey})
					_, _ = d.AddEdge(ctx, srcKey, "1")
				}
			},
			want: []string{"0", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(tt.d)
			cursor, err := tt.d.GetRoots(ctx)
			got := collector(t, cursor, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_DelVertex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		d         *arangodag.DAG
		name      string
		wantOrder int64
		wantSize  int64
		srcKey    string
		wantError error
		edgeCount int64
	}{
		{
			d:         someNewDag(t),
			name:      "unknown vertex",
			wantOrder: 0,
			wantSize:  0,
			srcKey:    "0",
			wantError: documentNotFoundError,
			edgeCount: 0,
		},
		{
			d:         standardDAG(t),
			name:      "two edges",
			wantOrder: 5,
			wantSize:  4,
			srcKey:    "0",
			edgeCount: 2,
		},
		{
			d:         standardDAG(t),
			name:      "no edges",
			wantOrder: 5,
			wantSize:  6,
			srcKey:    "5",
			edgeCount: 0,
		},
		{
			d:         standardDAG(t),
			name:      "in between",
			wantOrder: 5,
			wantSize:  4,
			srcKey:    "2",
			edgeCount: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edgeCount, err := tt.d.DelVertex(ctx, tt.srcKey)
			if err != tt.wantError {
				t.Fatalf("got %v, want %v", err, tt.wantError)
			}
			if edgeCount != tt.edgeCount {
				t.Errorf("got %v, want %v", edgeCount, tt.edgeCount)
			}
			gotOrder, errOrder := tt.d.GetOrder(ctx)
			if errOrder != nil {
				t.Error(errOrder)
			}
			if gotOrder != tt.wantOrder {
				t.Errorf("got %v, want %v", gotOrder, tt.wantOrder)
			}
			gotSize, errSize := tt.d.GetSize(ctx)
			if errSize != nil {
				t.Error(errSize)
			}
			if gotSize != tt.wantSize {
				t.Errorf("got %v, want %v", gotSize, tt.wantSize)
			}
		})
	}
}

func TestDAG_AddEdge(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name             string
		wantError        error
		wantArangoErr    bool
		wantArangoErrNum int
		srcKey           string
		dstKey           string
	}{
		{
			name:   "happy path",
			srcKey: "0",
			dstKey: "5",
		},
		{
			name:             "src doesn't exist",
			wantArangoErr:    true,
			wantArangoErrNum: 1202,
			srcKey:           "8",
			dstKey:           "1",
		},
		{
			name:             "dst doesn't exist",
			wantArangoErr:    true,
			wantArangoErrNum: 1202,
			srcKey:           "0",
			dstKey:           "6",
		},
		{
			name:             "duplicate edge",
			wantArangoErr:    true,
			wantArangoErrNum: 1210,
			srcKey:           "0",
			dstKey:           "1",
		},
		{
			name:      "loop",
			wantError: errors.New("loop"),
			srcKey:    "1",
			dstKey:    "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := d.AddEdge(ctx, tt.srcKey, tt.dstKey)
			if tt.wantArangoErr && !driver.IsArangoErrorWithErrorNum(got, tt.wantArangoErrNum) {
				t.Errorf("got %v, want arango Error with Num %d", got, tt.wantArangoErrNum)
			}
			if tt.wantError != nil && !reflect.DeepEqual(got, tt.wantError) {
				t.Errorf("got %v, want Error %v", got, tt.wantError)
			}
		})
	}
}

func TestDAG_AddEdgeUnchecked(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name             string
		wantError        error
		wantArangoErr    bool
		wantArangoErrNum int
		srcKey           string
		dstKey           string
	}{
		{
			name:   "happy path",
			srcKey: "0",
			dstKey: "5",
		},
		{
			name:   "src doesn't exist",
			srcKey: "8",
			dstKey: "1",
		},
		{
			name:   "dst doesn't exist",
			srcKey: "0",
			dstKey: "6",
		},
		{
			name:             "duplicate edge",
			wantArangoErr:    true,
			wantArangoErrNum: 1210,
			srcKey:           "0",
			dstKey:           "1",
		},
		{
			name:      "loop",
			wantError: errors.New("loop"),
			srcKey:    "1",
			dstKey:    "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := d.AddEdgeUnchecked(ctx, tt.srcKey, tt.dstKey)
			if tt.wantArangoErr && !driver.IsArangoErrorWithErrorNum(got, tt.wantArangoErrNum) {
				t.Errorf("got %v, want arango Error with Num %d", got, tt.wantArangoErrNum)
			}
			if tt.wantError != nil && !reflect.DeepEqual(got, tt.wantError) {
				t.Errorf("got %v, want Error %v", got, tt.wantError)
			}
		})
	}
}

func TestDAG_EdgeExists(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name   string
		want   bool
		srcKey string
		dstKey string
	}{
		{
			name:   "src does not exist",
			want:   false,
			srcKey: "6",
			dstKey: "0",
		},
		{
			name:   "dst does not exist",
			want:   false,
			srcKey: "0",
			dstKey: "6",
		},
		{
			name:   "neither src nor dest exist",
			want:   false,
			srcKey: "7",
			dstKey: "6",
		},
		{
			name:   "edge does not exist",
			want:   false,
			srcKey: "0",
			dstKey: "5",
		},
		{
			name:   "edge exists",
			want:   true,
			srcKey: "0",
			dstKey: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := d.EdgeExists(ctx, tt.srcKey, tt.dstKey)
			if err != nil {
				t.Error(err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_DelEdge(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name   string
		srcKey string
		dstKey string
	}{
		{
			name:   "src does not exist",
			srcKey: "6",
			dstKey: "0",
		},
		{
			name:   "dst does not exist",
			srcKey: "0",
			dstKey: "6",
		},
		{
			name:   "neither src nor dest exist",
			srcKey: "7",
			dstKey: "6",
		},
		{
			name:   "edge does not exist",
			srcKey: "0",
			dstKey: "5",
		},
		{
			name:   "edge exists",
			srcKey: "5",
			dstKey: "4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := d.DelEdge(ctx, tt.srcKey, tt.dstKey)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

func TestDAG_GetSize(t *testing.T) {
	t.Parallel()
	d := someNewDag(t)
	ctx := context.Background()
	got, err := d.GetSize(ctx)
	if err != nil {
		t.Error(err)
	}
	if got != 0 {
		t.Errorf("got %d, want %d", got, 0)
	}

	for i := 1; i <= 9; i++ {
		meta1, _ := d.AddVertex(ctx, idVertex{strconv.Itoa(i * 10)})
		meta2, _ := d.AddVertex(ctx, idVertex{strconv.Itoa(i*10 + 1)})
		_, _ = d.AddEdge(ctx, meta1.Key, meta2.Key)
		got, err = d.GetSize(ctx)
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
	ctx := context.Background()
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
		{
			name:   "src and dst are equal",
			want:   []string{"1"},
			srcKey: "1",
			dstKey: "1",
		},
		{
			name:   "dst doesn't exist",
			want:   nil,
			srcKey: "0",
			dstKey: "8",
		},
		{
			name:   "happy path",
			want:   []string{"2", "3", "4"},
			srcKey: "2",
			dstKey: "4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetShortestPath(ctx, tt.srcKey, tt.dstKey)
			if err != nil {
				t.Error(err)
			}
			got := collector(t, cursor, err)
			if err == nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDAG_GetParents(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
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
			name:   "src doesn't exist",
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
			cursor, err := d.GetParents(ctx, tt.srcKey)
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

func TestDAG_GetParentCount(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name   string
		want   int64
		srcKey string
	}{
		{
			name:   "src doesn't exist",
			want:   0,
			srcKey: "8",
		},
		{
			name:   "no parents",
			want:   0,
			srcKey: "0",
		},
		{
			name:   "one parent",
			want:   1,
			srcKey: "2",
		},
		{
			name:   "two parents",
			want:   2,
			srcKey: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := d.GetParentCount(ctx, tt.srcKey)
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
	ctx := context.Background()
	tests := []struct {
		name   string
		want   []string
		srcKey string
		dfs    bool
	}{
		{
			name:   "src doesn't exist",
			want:   nil,
			srcKey: "8",
			dfs:    false,
		},
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
			cursor, err := d.GetAncestors(ctx, tt.srcKey, tt.dfs)
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

func TestDAG_GetChildren(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name   string
		want   []string
		srcKey string
	}{
		{
			name:   "src doesn't exist",
			want:   nil,
			srcKey: "8",
		},
		{
			name:   "no children",
			want:   nil,
			srcKey: "4",
		},
		{
			name:   "one child",
			want:   []string{"4"},
			srcKey: "3",
		},
		{
			name:   "two children",
			want:   []string{"4", "2"},
			srcKey: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := d.GetChildren(ctx, tt.srcKey)
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

func TestDAG_GetChildCount(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	tests := []struct {
		name   string
		want   int64
		srcKey string
	}{
		{
			name:   "src doesn't exist",
			want:   0,
			srcKey: "8",
		},
		{
			name:   "no children",
			want:   0,
			srcKey: "4",
		},
		{
			name:   "one child",
			want:   1,
			srcKey: "3",
		},
		{
			name:   "two children",
			want:   2,
			srcKey: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := d.GetChildCount(ctx, tt.srcKey)
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
	ctx := context.Background()
	tests := []struct {
		name   string
		want   []string
		srcKey string
		dfs    bool
	}{
		{
			name:   "src doesn't exist",
			want:   nil,
			srcKey: "8",
		},
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
			cursor, err := d.GetDescendants(ctx, tt.srcKey, tt.dfs)
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

func TestDAG_String(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	ctx := context.Background()
	got, err := d.String(ctx)
	if err != nil {
		t.Error(err)
	}
	want := standardDot
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

const standardDot = `digraph  {
	
	n1[label="0"];
	n2[label="1"];
	n3[label="2"];
	n4[label="3"];
	n5[label="4"];
	n6[label="5"];
	n1->n2;
	n1->n4;
	n2->n3;
	n2->n5;
	n3->n4;
	n4->n5;
	
}
`

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

func someNewDag(t *testing.T) *arangodag.DAG {

	ctx := context.Background()

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

	uid := strconv.FormatInt(time.Now().UnixNano(), 10)

	d, err := arangodag.NewDAG(ctx, "test-"+uid, uid, client)
	if err != nil {
		t.Fatalf("failed to setup new dag: %v", err)
	}
	return d
}

type idVertex struct {
	Key string `json:"_key"`
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
	ctx := context.Background()
	_, _ = d.AddVertex(ctx, idVertex{"0"})
	_, _ = d.AddVertex(ctx, idVertex{"1"})
	_, _ = d.AddVertex(ctx, idVertex{"2"})
	_, _ = d.AddVertex(ctx, idVertex{"3"})
	_, _ = d.AddVertex(ctx, idVertex{"4"})
	_, _ = d.AddVertex(ctx, idVertex{"5"})
	_, _ = d.AddEdge(ctx, "0", "1")
	_, _ = d.AddEdge(ctx, "1", "2")
	_, _ = d.AddEdge(ctx, "1", "4")
	_, _ = d.AddEdge(ctx, "2", "3")
	_, _ = d.AddEdge(ctx, "3", "4")
	_, _ = d.AddEdge(ctx, "0", "3")
	return d
}
