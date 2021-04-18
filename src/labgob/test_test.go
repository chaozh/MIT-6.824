package labgob

import "testing"

import "bytes"

type T1 struct {
	T1int0    int
	T1int1    int
	T1string0 string
	T1string1 string
}

type T2 struct {
	T2slice []T1
	T2map   map[int]*T1
	T2t3    interface{}
}

type T3 struct {
	T3int999 int
}

//
// test that we didn't break GOB.
//
func TestGOB(t *testing.T) {
	e0 := errorCount

	w := new(bytes.Buffer)

	Register(T3{})

	{
		x0 := 0
		x1 := 1
		t1 := T1{}
		t1.T1int1 = 1
		t1.T1string1 = "6.824"
		t2 := T2{}
		t2.T2slice = []T1{T1{}, t1}
		t2.T2map = map[int]*T1{}
		t2.T2map[99] = &T1{1, 2, "x", "y"}
		t2.T2t3 = T3{999}

		e := NewEncoder(w)
		e.Encode(x0)
		e.Encode(x1)
		e.Encode(t1)
		e.Encode(t2)
	}
	data := w.Bytes()

	{
		var x0 int
		var x1 int
		var t1 T1
		var t2 T2

		r := bytes.NewBuffer(data)
		d := NewDecoder(r)
		if d.Decode(&x0) != nil ||
			d.Decode(&x1) != nil ||
			d.Decode(&t1) != nil ||
			d.Decode(&t2) != nil {
			t.Fatalf("Decode failed")
		}

		if x0 != 0 {
			t.Fatalf("wrong x0 %v\n", x0)
		}
		if x1 != 1 {
			t.Fatalf("wrong x1 %v\n", x1)
		}
		if t1.T1int0 != 0 {
			t.Fatalf("wrong t1.T1int0 %v\n", t1.T1int0)
		}
		if t1.T1int1 != 1 {
			t.Fatalf("wrong t1.T1int1 %v\n", t1.T1int1)
		}
		if t1.T1string0 != "" {
			t.Fatalf("wrong t1.T1string0 %v\n", t1.T1string0)
		}
		if t1.T1string1 != "6.824" {
			t.Fatalf("wrong t1.T1string1 %v\n", t1.T1string1)
		}
		if len(t2.T2slice) != 2 {
			t.Fatalf("wrong t2.T2slice len %v\n", len(t2.T2slice))
		}
		if t2.T2slice[1].T1int1 != 1 {
			t.Fatalf("wrong slice value\n")
		}
		if len(t2.T2map) != 1 {
			t.Fatalf("wrong t2.T2map len %v\n", len(t2.T2map))
		}
		if t2.T2map[99].T1string1 != "y" {
			t.Fatalf("wrong map value\n")
		}
		t3 := (t2.T2t3).(T3)
		if t3.T3int999 != 999 {
			t.Fatalf("wrong t2.T2t3.T3int999\n")
		}
	}

	if errorCount != e0 {
		t.Fatalf("there were errors, but should not have been")
	}
}

type T4 struct {
	Yes int
	no  int
}

//
// make sure we check capitalization
// labgob prints one warning during this test.
//
func TestCapital(t *testing.T) {
	e0 := errorCount

	v := []map[*T4]int{}

	w := new(bytes.Buffer)
	e := NewEncoder(w)
	e.Encode(v)
	data := w.Bytes()

	var v1 []map[T4]int
	r := bytes.NewBuffer(data)
	d := NewDecoder(r)
	d.Decode(&v1)

	if errorCount != e0+1 {
		t.Fatalf("failed to warn about lower-case field")
	}
}

//
// check that we warn when someone sends a default value over
// RPC but the target into which we're decoding holds a non-default
// value, which GOB seems not to overwrite as you'd expect.
//
// labgob does not print a warning.
//
func TestDefault(t *testing.T) {
	e0 := errorCount

	type DD struct {
		X int
	}

	// send a default value...
	dd1 := DD{}

	w := new(bytes.Buffer)
	e := NewEncoder(w)
	e.Encode(dd1)
	data := w.Bytes()

	// and receive it into memory that already
	// holds non-default values.
	reply := DD{99}

	r := bytes.NewBuffer(data)
	d := NewDecoder(r)
	d.Decode(&reply)

	if errorCount != e0+1 {
		t.Fatalf("failed to warn about decoding into non-default value")
	}
}
