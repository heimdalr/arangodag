package arangodag_test

import (
	"github.com/heimdalr/arangodag"
	"reflect"
	"testing"
)

const digraph = `digraph  {
	
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

func TestDAG_dump(t *testing.T) {
	t.Parallel()
	d := standardDAG(t)
	got, err := d.Dump()
	if err != nil {
		t.Error(err)
	}
	want := digraph
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
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