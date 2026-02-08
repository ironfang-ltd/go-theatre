package theatre

import (
	"sync"
	"testing"
)

func TestRingBuffer_WriteRead(t *testing.T) {
	rb := NewRingBuffer[int64](100)

	for i := 0; i < 1000; i++ {
		err := rb.Write(int64(i))
		if err != nil {
			t.Errorf("error writing to ring buffer: %v", err)
		}

		ii, ok := rb.Read()
		if !ok {
			t.Errorf("error reading from ring buffer: %v", err)
		}

		if ii != int64(i) {
			t.Errorf("expected %v, got %v", i, ii)
		}
	}
}

func TestRingBuffer_WriteReadN(t *testing.T) {
	rb := NewRingBuffer[int64](100)

	for i := 0; i < 10; i++ {
		err := rb.Write(int64(i))
		if err != nil {
			t.Errorf("error writing to ring buffer: %v", err)
		}
	}

	ii, ok := rb.ReadN(10)
	if !ok {
		t.Errorf("error reading from ring buffer")
	}

	for i := 0; i < 10; i++ {
		if ii[i] != int64(i) {
			t.Errorf("expected %v, got %v", i, ii[i])
		}
	}
}

func TestRingBuffer_ReadEmpty(t *testing.T) {
	rb := NewRingBuffer[int64](10)

	v, ok := rb.Read()
	if ok {
		t.Errorf("expected ok=false reading from empty buffer, got value %v", v)
	}
}

func TestRingBuffer_ReadNEmpty(t *testing.T) {
	rb := NewRingBuffer[int64](10)

	vals, ok := rb.ReadN(5)
	if ok {
		t.Errorf("expected ok=false reading from empty buffer, got %v", vals)
	}
	if vals != nil {
		t.Errorf("expected nil slice, got %v", vals)
	}
}

func TestRingBuffer_WriteFull(t *testing.T) {
	rb := NewRingBuffer[int64](5)

	for i := 0; i < 5; i++ {
		if err := rb.Write(int64(i)); err != nil {
			t.Fatalf("unexpected error on write %d: %v", i, err)
		}
	}

	err := rb.Write(99)
	if err != ErrRingBufferFull {
		t.Errorf("expected ErrRingBufferFull, got %v", err)
	}

	if rb.Len() != 5 {
		t.Errorf("expected len=5 after rejected write, got %d", rb.Len())
	}
}

func TestRingBuffer_Len(t *testing.T) {
	rb := NewRingBuffer[int64](10)

	if rb.Len() != 0 {
		t.Fatalf("expected len=0, got %d", rb.Len())
	}

	for i := 0; i < 5; i++ {
		rb.Write(int64(i))
	}

	if rb.Len() != 5 {
		t.Fatalf("expected len=5, got %d", rb.Len())
	}

	rb.Read()
	rb.Read()

	if rb.Len() != 3 {
		t.Fatalf("expected len=3, got %d", rb.Len())
	}

	rb.ReadN(3)

	if rb.Len() != 0 {
		t.Fatalf("expected len=0, got %d", rb.Len())
	}
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := NewRingBuffer[int64](4)

	// fill to capacity
	for i := 0; i < 4; i++ {
		rb.Write(int64(i))
	}

	// drain completely
	for i := 0; i < 4; i++ {
		v, ok := rb.Read()
		if !ok || v != int64(i) {
			t.Fatalf("pass 1: expected %d, got %d (ok=%v)", i, v, ok)
		}
	}

	// fill again — indices have wrapped
	for i := 10; i < 14; i++ {
		if err := rb.Write(int64(i)); err != nil {
			t.Fatalf("pass 2 write failed: %v", err)
		}
	}

	// verify correct order after wrap
	for i := 10; i < 14; i++ {
		v, ok := rb.Read()
		if !ok || v != int64(i) {
			t.Fatalf("pass 2: expected %d, got %d (ok=%v)", i, v, ok)
		}
	}
}

func TestRingBuffer_ReadNClamps(t *testing.T) {
	rb := NewRingBuffer[int64](10)

	for i := 0; i < 3; i++ {
		rb.Write(int64(i))
	}

	// request more than available
	vals, ok := rb.ReadN(100)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	for i := 0; i < 3; i++ {
		if vals[i] != int64(i) {
			t.Errorf("expected %d, got %d", i, vals[i])
		}
	}

	if rb.Len() != 0 {
		t.Errorf("expected len=0 after ReadN, got %d", rb.Len())
	}
}

func TestRingBuffer_ReadNWraparound(t *testing.T) {
	rb := NewRingBuffer[int64](4)

	// advance read/write indices to position 3
	for i := 0; i < 3; i++ {
		rb.Write(int64(i))
		rb.Read()
	}

	// now write 4 values that wrap around the underlying array
	for i := 0; i < 4; i++ {
		rb.Write(int64(i + 100))
	}

	vals, ok := rb.ReadN(4)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(vals) != 4 {
		t.Fatalf("expected 4 values, got %d", len(vals))
	}
	for i := 0; i < 4; i++ {
		if vals[i] != int64(i+100) {
			t.Errorf("index %d: expected %d, got %d", i, i+100, vals[i])
		}
	}
}

func TestRingBuffer_ConcurrentWriteRead(t *testing.T) {
	rb := NewRingBuffer[int64](256)
	count := 10000

	var wg sync.WaitGroup
	wg.Add(2)

	// writer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			for {
				if err := rb.Write(int64(i)); err == nil {
					break
				}
				// buffer full, spin
			}
		}
	}()

	// reader goroutine
	results := make([]int64, 0, count)
	go func() {
		defer wg.Done()
		for len(results) < count {
			v, ok := rb.Read()
			if ok {
				results = append(results, v)
			}
		}
	}()

	wg.Wait()

	if len(results) != count {
		t.Fatalf("expected %d results, got %d", count, len(results))
	}

	for i := 0; i < count; i++ {
		if results[i] != int64(i) {
			t.Fatalf("index %d: expected %d, got %d", i, i, results[i])
		}
	}
}
