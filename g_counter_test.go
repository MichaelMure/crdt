package crdt

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGCounter(t *testing.T) {
	for _, tt := range []struct {
		incsOne int
		incsTwo int
		result  int
	}{
		{5, 10, 15},
		{10, 5, 15},
		{100, 100, 200},
		{1, 2, 3},
	} {
		gOne, gTwo := NewGCounter(), NewGCounter()

		for i := 0; i < tt.incsOne; i++ {
			gOne.Inc()
		}

		for i := 0; i < tt.incsTwo; i++ {
			gTwo.Inc()
		}

		gOne.Merge(gTwo)

		if gOne.Count() != tt.result {
			t.Errorf("expected total count to be: %d, actual: %d",
				tt.result,
				gOne.Count())
		}

		gTwo.Merge(gOne)

		if gTwo.Count() != tt.result {
			t.Errorf("expected total count to be: %d, actual: %d",
				tt.result,
				gTwo.Count())
		}

	}
}

// test the JSON serialization of two, reconsistution. does it match?
// test the JSON serialization of two, and merger. do they turn into what was expected?
// test the JSON serialization of two, merging into a third, which already has some old/inaccurate merged values. Does it match?

func TestGCounterJSON(t *testing.T) {
	for _, tt := range []struct {
		incsOne int
		incsTwo int
		result  int
	}{
		{5, 10, 15},
		{10, 5, 15},
		{100, 100, 200},
		{1, 2, 3},
	} {
		gOne, gTwo := NewGCounter(), NewGCounter()

		for i := 0; i < tt.incsOne; i++ {
			gOne.Inc()
		}

		for i := 0; i < tt.incsTwo; i++ {
			gTwo.Inc()
		}

		out, err := gOne.MarshalJSON()
		fmt.Printf("Out One: %+v\n%+v\n", string(out), err)

		gOneImported := NewGCounterFromJSONBytes(out)

		// test the JSON serialization of one, reconsistution. does it match?
		if !reflect.DeepEqual(gOne, gOneImported) {
			t.Errorf("expected set to contain: %v, actual: %v", gOne, gOneImported)
		}

	}
}

func TestGCounterInvalidInput(t *testing.T) {
	gc := NewGCounter()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("panic expected here")
		}
	}()

	gc.IncVal(-5)
}
