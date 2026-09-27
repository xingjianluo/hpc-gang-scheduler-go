package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMemoryStoreRoundTrip(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if _, ok, err := s.Load(ctx); err != nil || ok {
		t.Fatalf("空存储 Load: ok=%v err=%v", ok, err)
	}
	if err := s.Save(ctx, Snapshot{Version: 7, Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if got.Version != 7 || string(got.Data) != "payload" {
		t.Fatalf("快照内容不符: %+v", got)
	}
}

func TestFileStoreRoundTripAndAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "snap.json")
	s := NewFileStore(path)
	ctx := context.Background()
	if _, ok, err := s.Load(ctx); err != nil || ok {
		t.Fatalf("文件不存在时 Load: ok=%v err=%v", ok, err)
	}
	for v := int64(1); v <= 5; v++ {
		if err := s.Save(ctx, Snapshot{Version: v, Data: []byte{byte(v)}}); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := s.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if got.Version != 5 || len(got.Data) != 1 || got.Data[0] != 5 {
		t.Fatalf("最终快照不符: %+v", got)
	}
}
