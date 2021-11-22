package arangodag

import (
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/go-test/deep"
	"os"
	"strconv"
	"testing"
	"time"
)

func someName() string {
	//return fmt.Sprintf("test_%s", uuid.New().String())
	return fmt.Sprintf("test_%d", time.Now().UnixNano())
}

func someNewDag(t *testing.T) *DAG {

	// get arangdb host and port from environment
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

	d, err := NewDAG(dbName, vertexCollName, edgeCollName, client)
	if err != nil {
		t.Fatalf("failed to setup new dag: %v", err)
	}
	return d
}

func TestNewDAG(t *testing.T) {
	someNewDag(t)
}

type idVertex struct {
	Key string `json:"_key"`
}

func TestDAG_AddVertex(t *testing.T) {
	d := someNewDag(t)

	// simple vertex
	autoId, err := d.AddVertex(struct{ foo string }{foo: "1"})
	if err != nil {
		t.Errorf("failed to AddVertex(): %v", err)
	}
	if autoId == "" {
		t.Errorf("want id, got: %v", autoId)
	}

	// vertex with id
	id := "1"
	idReturned, err := d.AddVertex(idVertex{Key: "1"})
	if err != nil {
		t.Fatalf("failed to AddVertex(): %v", err)
	}
	if idReturned != id {
		t.Errorf("AddVertex() = '%s', want %s", idReturned, id)
	}

	// duplicate
	_, errDuplicate := d.AddVertex(idVertex{Key: "1"})
	if errDuplicate == nil {
		t.Errorf("want duplicate Error")
	}

	// nil
	_, errNil := d.AddVertex(nil)
	if errNil == nil {
		t.Errorf("want nil Error")
	}

}

type foobar struct {
	A string
	B string
}

type foobarKey struct {
	A   string
	B   string
	Key string `json:"_key"`
}

func TestDAG_GetVertex(t *testing.T) {
	d := someNewDag(t)

	v0 := idVertex{Key: "1"}
	k, _ := d.AddVertex(v0)
	var v1 idVertex
	err := d.GetVertex(k, &v1)
	if err != nil {
		t.Errorf("failed to GetVertex(): %v", err)
	}
	if deep.Equal(v0, v1) != nil {
		t.Errorf("GetVertex() = %v, want %v", v1, v0)
	}

	// "complex" document without key
	v2 := foobar{A: "foo", B: "bar"}
	var v3 foobar
	k2, _ := d.AddVertex(v2)
	err = d.GetVertex(k2, &v3)
	if err != nil {
		t.Errorf("failed to GetVertex(): %v", err)
	}
	if deep.Equal(v2, v3) != nil {
		t.Errorf("GetVertex() = %v, want %v", v3, v2)
	}

	// "complex" document with key
	v4 := foobarKey{A: "foo", B: "bar", Key: "myFancyKey"}
	var v5 foobarKey
	k4, _ := d.AddVertex(v4)
	err = d.GetVertex(k4, &v5)
	if err != nil {
		t.Errorf("failed to GetVertex(): %v", err)
	}
	if deep.Equal(v4, v5) != nil {
		t.Errorf("GetVertex() = %v, want %v", v5, v4)
	}

	// unknown
	var v idVertex
	errUnknown := d.GetVertex("foo", v)
	if errUnknown == nil {
		t.Errorf("want document not found")
	}

	// empty
	errEmpty := d.GetVertex("", v)
	if errEmpty == nil {
		t.Errorf("want key is empty")
	}
}

func TestDAG_GetOrder(t *testing.T) {
	d := someNewDag(t)
	order, err := d.GetOrder()
	if err != nil {
		t.Errorf("failed to GetOrder(): %v", err)
	}
	if order != 0 {
		t.Errorf("GetOrder() = %d, want %d", order, 0)
	}

	for i := 1; i <= 10; i++ {
		_, _ = d.AddVertex(idVertex{Key: strconv.Itoa(i)})
		order, err = d.GetOrder()
		if err != nil {
			t.Errorf("failed to GetOrder(): %v", err)
		}
		if int(order) != i {
			t.Errorf("GetOrder() = %d, want %d", order, 1)
		}
	}
}

func TestDAG_AddEdge(t *testing.T) {
	d := someNewDag(t)
	k1, _ := d.AddVertex(idVertex{"0"})
	k2, _ := d.AddVertex(idVertex{"1"})

	// adding edge
	errAddEdge := d.AddEdge(k1, k2)
	if errAddEdge != nil {
		t.Errorf("AddEdge() failed: %v", errAddEdge)
	}

	// after adding edge
	isEdge, errIsEdge := d.IsEdge(k1, k2)
	if errIsEdge != nil {
		t.Errorf("IsEdge() failed: %v", errIsEdge)
	}
	if !isEdge {
		t.Errorf("IsEdge() = %t, want %t", isEdge, true)
	}
	size, errSize := d.GetSize()
	if errSize != nil {
		t.Errorf("GetSize() failed: %v", errSize)
	}
	if size != 1 {
		t.Errorf("GetSize() = %d, want 1", size)
	}

	// adding duplicate
	errAddEdgeDuplicate := d.AddEdge(k1, k2)
	if errAddEdgeDuplicate == nil {
		t.Errorf("AddEdge() succeeded, want error")
	}

	// adding edge for unknown vertex
	errAddEdgeUnknown := d.AddEdge(k1, "3")
	if errAddEdgeUnknown == nil {
		t.Errorf("AddEdge() succeeded, want error")
	}

	// loop
	errAddEdgeLoop := d.AddEdge(k2, k1)
	if errAddEdgeLoop == nil {
		t.Errorf("AddEdge() succeeded, want error")
	}

	// loop self
	errAddEdgeLoopSelf := d.AddEdge(k2, k2)
	if errAddEdgeLoopSelf == nil {
		t.Errorf("AddEdge() succeeded, want error")
	}

}

func TestDAG_IsEdge(t *testing.T) {
	d := someNewDag(t)
	k1, _ := d.AddVertex(idVertex{"0"})
	k2, _ := d.AddVertex(idVertex{"1"})

	// before adding the edge
	isEdge, errIsEdge := d.IsEdge(k1, k2)
	if errIsEdge != nil {
		t.Errorf("IsEdge() failed: %v", errIsEdge)
	}
	if isEdge {
		t.Errorf("IsEdge() = %t, want %t", isEdge, false)
	}

	// adding edge
	errAddEdge := d.AddEdge(k1, k2)
	if errAddEdge != nil {
		t.Errorf("AddEdge() failed: %v", errAddEdge)
	}

	// after adding edge
	isEdge2, errIsEdge2 := d.IsEdge(k1, k2)
	if errIsEdge2 != nil {
		t.Errorf("IsEdge() failed: %v", errIsEdge2)
	}
	if !isEdge2 {
		t.Errorf("IsEdge() = %t, want %t", isEdge2, true)
	}
}

func TestDAG_GetSize(t *testing.T) {
	d := someNewDag(t)
	size, err := d.GetSize()
	if err != nil {
		t.Errorf("failed to GetSize(): %v", err)
	}
	if size != 0 {
		t.Errorf("GetSize() = %d, want %d", size, 0)
	}

	for i := 1; i <= 9; i++ {
		id1, _ := d.AddVertex(idVertex{strconv.Itoa(i * 10)})
		id2, _ := d.AddVertex(idVertex{strconv.Itoa(i*10 + 1)})
		_ = d.AddEdge(id1, id2)
		size, err := d.GetSize()
		if err != nil {
			t.Errorf("failed to GetSize(): %v", err)
		}
		if int(size) != i {
			t.Errorf("GetSize() = %d, want %d", size, 1)
		}
	}
}


func TestDAG_GetShortestPath(t *testing.T) {
	d := someNewDag(t)
	_, _ = d.AddVertex(idVertex{"0"})
	_, _ = d.AddVertex(idVertex{"1"})
	_, _ = d.AddVertex(idVertex{"2"})
	_, _ = d.AddVertex(idVertex{"3"})
	_, _ = d.AddVertex(idVertex{"4"})
	_ = d.AddEdge("0", "1")
	_ = d.AddEdge("1", "2")
	_ = d.AddEdge("2", "3")
	_ = d.AddEdge("3", "4")

	// path exists
	path, err := d.GetShortestPath("0", "4")
	if err != nil {
		t.Errorf("failed to GetShortestPath(): %v", err)
	}
	want := []string{"0", "1", "2", "3", "4"}
	if deep.Equal(path, want) != nil {
		t.Errorf("GetShortestPath() = %v, want %v", path, want)
	}

	// path doesn't exist
	_, _ = d.AddVertex(idVertex{"5"})
	path2, err2 := d.GetShortestPath("0", "5")
	if err2 != nil {
		t.Errorf("failed to GetShortestPath(): %v", err2)
	}
	var want2 []string
	if deep.Equal(path2, want2) != nil {
		t.Errorf("GetShortestPath() = %v, want %v", path2, want2)
	}

	// alternate path
	_ = d.AddEdge("0", "3")
	path3, err3 := d.GetShortestPath("0", "4")
	if err3 != nil {
		t.Errorf("failed to GetShortestPath(): %v", err3)
	}
	want3 := []string{"0", "3", "4"}
	if deep.Equal(path3, want3) != nil {
		t.Errorf("GetShortestPath() = %v, want %v", path3, want3)
	}

	// 2 shortest paths pick the BFS first one
	_ = d.AddEdge("1", "4")
	path4, err4 := d.GetShortestPath("0", "4")
	if err4 != nil {
		t.Errorf("failed to GetShortestPath(): %v", err4)
	}
	want4 := []string{"0", "1", "4"}
	if deep.Equal(path4, want4) != nil {
		t.Errorf("GetShortestPath() = %v, want %v", path4, want4)
	}
}


func TestDAG_WalkAncestors(t *testing.T) {
	d := someNewDag(t)
	_, _ = d.AddVertex(idVertex{"0"})
	_, _ = d.AddVertex(idVertex{"1"})
	_, _ = d.AddVertex(idVertex{"2"})
	_, _ = d.AddVertex(idVertex{"3"})
	_, _ = d.AddVertex(idVertex{"4"})
	_ = d.AddEdge("0", "1")
	_ = d.AddEdge("1", "2")
	_ = d.AddEdge("2", "3")
	_ = d.AddEdge("3", "4")


	// simple chain BFS
	var collect []string
	_ = d.WalkAncestors("4", func(key string, err error) error {
		collect = append(collect, key)
		return nil
	}, false)
	want := []string{"3", "2", "1", "0"}
	if deep.Equal(collect, want) != nil {
		t.Errorf("WalkAncestors() = %v, want %v", collect, want)
	}


	// two parents BFS
	var collect2 []string
	key, _ := d.AddVertex(idVertex{"0a"})
	_ = d.AddEdge(key, "1")
	_ = d.WalkAncestors("4", func(key string, err error) error {
		collect2 = append(collect2, key)
		return nil
	}, false)
	want2 := []string{"3", "2", "1", "0a", "0"}
	if deep.Equal(collect2, want2) != nil {
		t.Errorf("WalkAncestors() = %v, want %v", collect2, want2)
	}

	// rhombus BFS
	var collect3 []string
	key, _ = d.AddVertex(idVertex{"2a"})
	_ = d.AddEdge("1", key)
	_ = d.AddEdge(key, "3")
	_ = d.WalkAncestors("4", func(key string, err error) error {
		collect3 = append(collect3, key)
		return nil
	}, false)
	want3 := []string{"3", "2a", "2", "1", "0a", "0"}
	if deep.Equal(collect3, want3) != nil {
		t.Errorf("WalkAncestors() = %v, want %v", collect3, want3)
	}

	// rhombus DFS
	var collect4 []string
	_ = d.WalkAncestors("4", func(key string, err error) error {
		collect4 = append(collect4, key)
		return nil
	}, true)
	want4 := []string{"3", "2", "1", "0", "0a", "2a"}
	if deep.Equal(collect4, want4) != nil {
		t.Errorf("WalkAncestors() = %v, want %v", collect3, want3)
	}
}

func TestDAG_GetLeaves(t *testing.T) {
	d := someNewDag(t)
	_, _ = d.AddVertex(idVertex{"0"})

	// start is leave
	leaves, err := d.GetLeaves()
	if err != nil {
		t.Errorf("failed to GetLeaves(): %v", err)
	}
	want := []string{"0"}
	if deep.Equal(leaves, want) != nil {
		t.Errorf("GetLeaves() = %v, want %v", leaves, want)
	}

	_, _ = d.AddVertex(idVertex{"1"})
	_ = d.AddEdge("0", "1")

	// one "real" leave
	leaves2, err2 := d.GetLeaves()
	if err2 != nil {
		t.Errorf("failed to GetLeaves(): %v", err2)
	}
	want2 := []string{"1"}
	if deep.Equal(leaves2, want2) != nil {
		t.Errorf("GetLeaves() = %v, want %v", leaves2, want2)
	}

	// 10 leaves
	for i := 2; i < 10; i++ {
		dstKey := strconv.Itoa(i)
		_, _ = d.AddVertex(idVertex{dstKey})
		_ = d.AddEdge("0", dstKey)
	}
	leaves3, err3 := d.GetLeaves()
	if err3 != nil {
		t.Errorf("failed to GetLeaves(): %v", err3)
	}
	want3 := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	if deep.Equal(leaves3, want3) != nil {
		t.Errorf("GetLeaves() = %v, want %v", leaves3, want3)
	}
}


func TestDAG_GetRoots(t *testing.T) {
	d := someNewDag(t)
	_, _ = d.AddVertex(idVertex{"0"})

	// start is root
	roots, err := d.GetRoots()
	if err != nil {
		t.Errorf("failed to GetRoots(): %v", err)
	}
	want := []string{"0"}
	if deep.Equal(roots, want) != nil {
		t.Errorf("GetRoots() = %v, want %v", roots, want)
	}

	_, _ = d.AddVertex(idVertex{"1"})
	_ = d.AddEdge("0", "1")

	// one "real" root
	roots2, err2 := d.GetRoots()
	if err2 != nil {
		t.Errorf("failed to GetRoots(): %v", err2)
	}
	want2 := []string{"0"}
	if deep.Equal(roots2, want2) != nil {
		t.Errorf("GetRoots() = %v, want %v", roots2, want2)
	}

	// 9 roots
	for i := 2; i < 10; i++ {
		srcKey := strconv.Itoa(i)
		_, _ = d.AddVertex(idVertex{srcKey})
		_ = d.AddEdge(srcKey, "1")
	}
	roots3, err3 := d.GetRoots()
	if err3 != nil {
		t.Errorf("failed to GetRoots(): %v", err3)
	}
	want3 := []string{"0", "2", "3", "4", "5", "6", "7", "8", "9"}
	if deep.Equal(roots3, want3) != nil {
		t.Errorf("GetRoots() = %v, want %v", roots3, want3)
	}
}


/*
func DeleteVertexTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)

	// delete a single vertex and inspect the graph
	_ = d.DeleteVertex(k1)
	if order, _ := d.GetOrder(); order != 0 {
		t.Errorf("GetOrder() = %d, want 0", order)
	}
	if size, _ := d.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
	if leaves, _ := d.GetLeaves(); len(leaves) != 0 {
		t.Errorf("GetLeaves() = %d, want 0", len(leaves))
	}
	if roots, _ := d.GetRoots(); len(roots) != 0 {
		t.Errorf("GetLeaves() = %d, want 0", len(roots))
	}
	if vertices, _ := d.GetVertices(); len(vertices) != 0 {
		t.Errorf("GetVertices() = %d, want 0", len(vertices))
	}

	k1, _ = d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	if order, _ := d.GetOrder(); order != 3 {
		t.Errorf("GetOrder() = %d, want 3", order)
	}
	if size, _ := d.GetSize(); size != 2 {
		t.Errorf("GetSize() = %d, want 2", size)
	}
	if leaves, _ := d.GetLeaves(); len(leaves) != 1 {
		t.Errorf("GetLeaves() = %d, want 1", len(leaves))
	}
	if roots, _ := d.GetRoots(); len(roots) != 1 {
		t.Errorf("GetLeaves() = %d, want 1", len(roots))
	}
	if vertices, _ := d.GetVertices(); len(vertices) != 3 {
		t.Errorf("GetVertices() = %d, want 3", len(vertices))
	}
	if vertices, _ := d.GetDescendants(k1); len(vertices) != 2 {
		t.Errorf("GetDescendants(v1) = %d, want 2", len(vertices))
	}
	if vertices, _ := d.GetAncestors(k3); len(vertices) != 2 {
		t.Errorf("GetAncestors(v3) = %d, want 2", len(vertices))
	}

	_ = d.DeleteVertex(k2)
	if order, _ := d.GetOrder(); order != 2 {
		t.Errorf("GetOrder() = %d, want 2", order)
	}
	if size, _ := d.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}
	if leaves, _ := d.GetLeaves(); len(leaves) != 2 {
		t.Errorf("GetLeaves() = %d, want 2", len(leaves))
	}
	if roots, _ := d.GetRoots(); len(roots) != 2 {
		t.Errorf("GetLeaves() = %d, want 2", len(roots))
	}
	if vertices, _ := d.GetVertices(); len(vertices) != 2 {
		t.Errorf("GetVertices() = %d, want 2", len(vertices))
	}
	if vertices, _ := d.GetDescendants(k1); len(vertices) != 0 {
		t.Errorf("GetDescendants(v1) = %d, want 0", len(vertices))
	}
	if vertices, _ := d.GetAncestors(k3); len(vertices) != 0 {
		t.Errorf("GetAncestors(v3) = %d, want 0", len(vertices))
	}

	// unknown
	errUnknown := d.DeleteVertex("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("DeleteVertex(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	errEmpty := d.DeleteVertex("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("DeleteVertex(\"\") = '%v', want empty key error", errEmpty)
	}
}

func AddEdgeTest(d DAG, t *testing.T) {
	k0, _ := d.AddVertex(0)
	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)

	// add a single edge and inspect the graph
	_ = d.AddEdge(k1, k2)
	if children, _ := d.GetChildren(k1); len(children) != 1 {
		t.Errorf("GetChildren(k1) = %d, want 1", len(children))
	}
	if parents, _ := d.GetParents(k2); len(parents) != 1 {
		t.Errorf("GetParents(k2) = %d, want 1", len(parents))
	}
	if leaves, _ := d.GetLeaves(); len(leaves) != 3 {
		t.Errorf("GetLeaves() = %d, want 3", len(leaves))
	}
	if roots, _ := d.GetRoots(); len(roots) != 3 {
		t.Errorf("GetRoots() = %d, want 3", len(roots))
	}
	if vertices, _ := d.GetDescendants(k1); len(vertices) != 1 {
		t.Errorf("GetDescendants(k1) = %d, want 1", len(vertices))
	}
	if vertices, _ := d.GetAncestors(k2); len(vertices) != 1 {
		t.Errorf("GetAncestors(k2) = %d, want 1", len(vertices))
	}

	err := d.AddEdge(k2, k3)
	if err != nil {
		t.Fatal(err)
	}
	if vertices, _ := d.GetDescendants(k1); len(vertices) != 2 {
		t.Errorf("GetDescendants(k1) = %d, want 2", len(vertices))
	}
	if vertices, _ := d.GetAncestors(k3); len(vertices) != 2 {
		t.Errorf("GetAncestors(k3) = %d, want 2", len(vertices))
	}

	_ = d.AddEdge(k0, k1)
	if vertices, _ := d.GetDescendants(k0); len(vertices) != 3 {
		t.Errorf("GetDescendants(k0) = %d, want 3", len(vertices))
	}
	if vertices, _ := d.GetAncestors(k3); len(vertices) != 3 {
		t.Errorf("GetAncestors(k3) = %d, want 3", len(vertices))
	}

	// loop
	errLoopSrcSrc := d.AddEdge(k1, k1)
	if ! IsSrcDstEqualError(errLoopSrcSrc) {
		t.Errorf("AddEdge(k1, k1) = '%v', want loop error", errLoopSrcSrc)
	}

	errLoopDstSrc := d.AddEdge(k2, k1)
	if ! IsLoopError(errLoopDstSrc) {
		t.Errorf("AddEdge(k2, k1) = '%v', want loop error", errLoopDstSrc)
	}

	// duplicate
	errDuplicate := d.AddEdge(k1, k2)
	if ! IsDuplicateEdgeError(errDuplicate) {
		t.Errorf("AddEdge(k1, k2) = '%v', want duplicate edge error", errDuplicate)
	}

	// empty
	errEmptySrc := d.AddEdge("", k2)
	if ! IsEmptyIDError(errEmptySrc) {
		t.Errorf("AddEdge(\"\", k2) = '%v', want empty key error", errEmptySrc)
	}
	errEmptyDst := d.AddEdge(k1, "")
	if ! IsEmptyIDError(errEmptyDst) {
		t.Errorf("AddEdge(k1, \"\") = '%v', want empty key error", errEmptyDst)
	}
}

func DeleteEdgeTest(d DAG, t *testing.T) {

	k0, _ := d.AddVertex(0)
	k1, _ := d.AddVertex(1)
	_ = d.AddEdge(k0, k1)
	if size, _ := d.GetSize(); size != 1 {
		t.Errorf("GetSize() = %d, want 1", size)
	}
	_ = d.DeleteEdge(k0, k1)
	if size, _ := d.GetSize(); size != 0 {
		t.Errorf("GetSize() = %d, want 0", size)
	}

	// unknown
	errUnknown := d.DeleteEdge(k0, k1)
	if ! IsUnknownEdgeError(errUnknown) {
		t.Errorf("DeleteEdge(k0, k1) = '%v', want unknown edge error", errUnknown)
	}

	// empty
	errEmptySrc := d.DeleteEdge("", k1)
	if ! IsEmptyIDError(errEmptySrc) {
		t.Errorf("DeleteEdge(\"\", k1) = '%v', want empty key error", errEmptySrc)
	}
	errEmptyDst := d.DeleteEdge(k0, "")
	if ! IsEmptyIDError(errEmptyDst) {
		t.Errorf("DeleteEdge(k0, \"\") = '%v', want empty key error", errEmptyDst)
	}

	// unknown

	// unknown
	errUnknownSrc := d.DeleteEdge("foo", k1)
	if ! IsUnknownIDError(errUnknownSrc) {
		t.Errorf("DeleteEdge(\"foo\", k1) = '%v', want unknown key error", errUnknownSrc)
	}

	errUnknownDst := d.DeleteEdge(k0, "foo")
	if ! IsUnknownIDError(errUnknownDst) {
		t.Errorf("DeleteEdge(k0, \"foo\") = '%v', want unknown key error", errUnknownDst)
	}
}

func GetChildrenTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k1, k3)

	children, _ := d.GetChildren(k1)
	if length := len(children); length != 2 {
		t.Errorf("GetChildren() = %d, want 2", length)
	}
	if _, exists := children[k2]; !exists {
		t.Error("GetChildren()[k2] = false, want true")
	}
	if _, exists := children[k3]; !exists {
		t.Error("GetChildren()[k3] = false, want true")
	}

	// unknown
	_, errUnknown := d.GetChildren("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetChildren(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetChildren("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetChildren(\"\") = '%v', want empty key error", errEmpty)
	}


}

func GetParentsTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)

	_ = d.AddEdge(k1, k3)
	_ = d.AddEdge(k2, k3)

	parents, _ := d.GetParents(k3)
	if length := len(parents); length != 2 {
		t.Errorf("GetParents(k3) = %d, want 2", length)
	}
	if _, exists := parents[k1]; !exists {
		t.Errorf("GetParents(k3)[k1] = %t, want true", exists)
	}
	if _, exists := parents[k2]; !exists {
		t.Errorf("GetParents(k3)[k2] = %t, want true", exists)
	}

	// unknown
	_, errUnknown := d.GetParents("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetParents(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetParents("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetParents(\"\") = '%v', want empty key error", errEmpty)
	}

}

func GetDescendantsTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)

	if desc, _ := d.GetDescendants(k1); len(desc) != 3 {
		t.Errorf("GetDescendants(k1) = %d, want 3", len(desc))
	}
	if desc, _ := d.GetDescendants(k2); len(desc) != 2 {
		t.Errorf("GetDescendants(k2) = %d, want 2", len(desc))
	}
	if desc, _ := d.GetDescendants(k3); len(desc) != 0 {
		t.Errorf("GetDescendants(k4) = %d, want 0", len(desc))
	}
	if desc, _ := d.GetDescendants(k4); len(desc) != 0 {
		t.Errorf("GetDescendants(k4) = %d, want 0", len(desc))
	}

	// unknown
	_, errUnknown := d.GetDescendants("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetDescendants(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetDescendants("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetDescendants(\"\") = '%v', want empty key error", errEmpty)
	}
}

func GetOrderedDescendantsTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)

	if desc, _ := d.GetOrderedDescendants(k1); len(desc) != 3 {
		t.Errorf("GetOrderedDescendants(k1) = %d, want 3", len(desc))
	}
	if desc, _ := d.GetOrderedDescendants(k2); len(desc) != 2 {
		t.Errorf("GetOrderedDescendants(k2) = %d, want 2", len(desc))
	}
	if desc, _ := d.GetOrderedDescendants(k3); len(desc) != 0 {
		t.Errorf("GetOrderedDescendants(k4) = %d, want 0", len(desc))
	}
	if desc, _ := d.GetOrderedDescendants(k4); len(desc) != 0 {
		t.Errorf("GetOrderedDescendants(k4) = %d, want 0", len(desc))
	}
	if desc, _ := d.GetOrderedDescendants(k1); !equal(desc, []string{k2, k3, k4}) && !equal(desc, []string{k2, k4, k3}) {
		t.Errorf("GetOrderedDescendants(k4) = %v, want %v or %v", desc, []string{k2, k3, k4}, []string{k2, k4, k3})
	}

	// unknown
	_, errUnknown := d.GetOrderedDescendants("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetOrderedDescendants(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetOrderedDescendants("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetOrderedDescendants(\"\") = '%v', want empty key error", errEmpty)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func GetAncestorsTest(d DAG, t *testing.T) {

	k0, _ := d.AddVertex(0)
	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)
	k5, _ := d.AddVertex(5)
	k6, _ := d.AddVertex(6)
	k7, _ := d.AddVertex(7)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)

	if ancestors, _ := d.GetAncestors(k4); len(ancestors) != 2 {
		t.Errorf("GetAncestors(k4) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := d.GetAncestors(k3); len(ancestors) != 2 {
		t.Errorf("GetAncestors(k3) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := d.GetAncestors(k2); len(ancestors) != 1 {
		t.Errorf("GetAncestors(k2) = %d, want 1", len(ancestors))
	}
	if ancestors, _ := d.GetAncestors(k1); len(ancestors) != 0 {
		t.Errorf("GetAncestors(k1) = %d, want 0", len(ancestors))
	}

	_ = d.AddEdge(k3, k5)
	_ = d.AddEdge(k4, k6)

	if ancestors, _ := d.GetAncestors(k4); len(ancestors) != 2 {
		t.Errorf("GetAncestors(k4) = %d, want 2", len(ancestors))
	}
	if ancestors, _ := d.GetAncestors(k7); len(ancestors) != 0 {
		t.Errorf("GetAncestors(k4) = %d, want 7", len(ancestors))
	}
	_ = d.AddEdge(k5, k7)
	if ancestors, _ := d.GetAncestors(k7); len(ancestors) != 4 {
		t.Errorf("GetAncestors(k7) = %d, want 4", len(ancestors))
	}
	_ = d.AddEdge(k0, k1)
	if ancestors, _ := d.GetAncestors(k7); len(ancestors) != 5 {
		t.Errorf("GetAncestors(k7) = %d, want 5", len(ancestors))
	}

	// unknown
	_, errUnknown := d.GetAncestors("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetAncestors(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetAncestors("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetAncestors(\"\") = '%v', want empty key error", errEmpty)
	}

}

func GetOrderedAncestorsTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)

	if desc, _ := d.GetOrderedAncestors(k4); len(desc) != 2 {
		t.Errorf("GetOrderedAncestors(k4) = %d, want 2", len(desc))
	}
	if desc, _ := d.GetOrderedAncestors(k2); len(desc) != 1 {
		t.Errorf("GetOrderedAncestors(k2) = %d, want 1", len(desc))
	}
	if desc, _ := d.GetOrderedAncestors(k1); len(desc) != 0 {
		t.Errorf("GetOrderedAncestors(k1) = %d, want 0", len(desc))
	}
	if desc, _ := d.GetOrderedAncestors(k4); !equal(desc, []string{k2, k1}) {
		t.Errorf("GetOrderedAncestors(k4) = %v, want %v", desc, []string{k2, k1})
	}

	// unknown
	_, errUnknown := d.GetOrderedAncestors("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("GetOrderedAncestors(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, errEmpty := d.GetOrderedAncestors("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("GetOrderedAncestors(\"\") = '%v', want empty key error", errEmpty)
	}
}

func AncestorsWalkerTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)
	k5, _ := d.AddVertex(5)
	k6, _ := d.AddVertex(6)
	k7, _ := d.AddVertex(7)
	k8, _ := d.AddVertex(8)
	k9, _ := d.AddVertex(9)
	k10, _ := d.AddVertex(10)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k1, k3)
	_ = d.AddEdge(k2, k4)
	_ = d.AddEdge(k2, k5)
	_ = d.AddEdge(k4, k6)
	_ = d.AddEdge(k5, k6)
	_ = d.AddEdge(k6, k7)
	_ = d.AddEdge(k7, k8)
	_ = d.AddEdge(k7, k9)
	_ = d.AddEdge(k8, k10)
	_ = d.AddEdge(k9, k10)

	vertices, _, _ := d.AncestorsWalker(k10)
	var ancestors []string
	for v := range vertices {
		ancestors = append(ancestors, v)
	}
	exp1 := []string{k9, k8, k7, k6, k4, k5, k2, k1}
	exp2 := []string{k8, k9, k7, k6, k4, k5, k2, k1}
	exp3 := []string{k9, k8, k7, k6, k5, k4, k2, k1}
	exp4 := []string{k8, k9, k7, k6, k5, k4, k2, k1}
	if !(equal(ancestors, exp1) || equal(ancestors, exp2) || equal(ancestors, exp3) || equal(ancestors, exp4)) {
		t.Errorf("AncestorsWalker(k10) = %v, want %v, %v, %v, or %v ", ancestors, exp1, exp2, exp3, exp4)
	}

	// unknown
	_, _, errUnknown := d.AncestorsWalker("foo")
	if ! IsUnknownIDError(errUnknown) {
		t.Errorf("AncestorsWalker(\"foo\") = '%v', want unknown key error", errUnknown)
	}

	// empty
	_, _, errEmpty := d.AncestorsWalker("")
	if ! IsEmptyIDError(errEmpty) {
		t.Errorf("AncestorsWalker(\"\") = '%v', want empty key error", errEmpty)
	}
}

func AncestorsWalkerSignalTest(d DAG, t *testing.T) {
	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)
	k5, _ := d.AddVertex(5)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)
	_ = d.AddEdge(k4, k5)

	var ancestors []string
	vertices, signal, _ := d.AncestorsWalker(k5)
	for v := range vertices {
		ancestors = append(ancestors, v)
		if v == k2 {
			signal <- true
			break
		}
	}
	if !equal(ancestors, []string{k4, k2}) {
		t.Errorf("AncestorsWalker(k4) = %v, want %v", ancestors, []string{k4, k2})
	}
}

func GetStringTest(d DAG, t *testing.T) {

	k1, _ := d.AddVertex(1)
	k2, _ := d.AddVertex(2)
	k3, _ := d.AddVertex(3)
	k4, _ := d.AddVertex(4)

	_ = d.AddEdge(k1, k2)
	_ = d.AddEdge(k2, k3)
	_ = d.AddEdge(k2, k4)
	expected := "DAG Vertices: 4 - Edges: 3"
	s := d.String()
	if s[:len(expected)] != expected {
		t.Errorf("String() = \"%s\", want \"%s\"", s, expected)
	}
}

func InterfaceTests(t *testing.T, newFn func() dag.DAG, tests []func(dag.DAG, *testing.T)) {

	tests := []struct {
		name string
		fn   func(dag.DAG, *testing.T)
	}{
		{name: "NewDAG()", fn: newDAG},
		{name: "AddVertex()", fn: addVertex},
		{name: "GetVertex()", fn: getVertex},
		{name: "DeleteVertex()", fn: deleteVertex},
		{name: "AddEdge()", fn: addEdge},
		{name: "DeleteEdge()", fn: deleteEdge},
		{name: "GetChildren()", fn: getChildren},
		{name: "GetParents()", fn: getParents},
		{name: "GetDescendants()", fn: getDescendants},
		{name: "GetOrderedDescendants()", fn: getOrderedDescendants},
		{name: "GetAncestors()", fn: getAncestors},
		{name: "GetOrderedAncestors()", fn: getOrderedAncestors},
		{name: "AncestorsWalker()", fn: ancestorsWalker},
		{name: "AncestorsWalkerSignal()", fn: ancestorsWalkerSignal},
		{name: "ReduceTransitively()", fn: reduceTransitively},
		{name: "String()", fn: getString},
	}



	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					log.Println("panic occurred:", err)
				}
			}()
			test.fn(newFn(), t) })
	}
}

*/
