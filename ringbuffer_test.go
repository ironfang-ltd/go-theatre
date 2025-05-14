package theatre

import "testing"

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
