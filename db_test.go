package corekv

import (
	"testing"
	"time"

	"github.com/qingw1230/corekv/utils"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	e := utils.NewEntry([]byte("hello"), []byte("coreKV")).WithTTL(1 * time.Second)
	if err := db.Set(e); err != nil {
		t.Fatal(err)
	}
	if entry, err := db.Get([]byte("hello")); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
	}
	iter := db.NewIterator(&utils.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}
}

func FuzzAPI(f *testing.F) {
	f.Add([]byte("core"), []byte("kv"))
	clearDir()
	db := Open(opt)
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = 1 << 10
	defer func() { _ = db.Close() }()
	f.Fuzz(func(t *testing.T, key, value []byte) {
		e := utils.NewEntry(key, value).WithTTL(100 * time.Second)
		if err := db.Set(e); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.Set key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		}
		if entry, err := db.Get(key); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.Get key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
		iter := db.NewIterator(&utils.Options{
			IsAsc: false,
		})
		defer func() { _ = iter.Close() }()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			it := iter.Item()
			t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
		}
		t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
		if err := db.Del(key); err != nil {
			if err != utils.ErrEmptyKey {
				t.Fatalf("db.del key=%s, value=%s, expiresAt=%d, err=%+v", e.Key, e.Value, e.ExpiresAt, err)
			}
		}
	})
}
