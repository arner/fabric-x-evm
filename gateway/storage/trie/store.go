/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package trie

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Store wraps the geth TrieDB to keep track of a full MPT of the world state.
// When dbPath is non-empty it is backed by PebbleDB and survives restarts.
// When dbPath is empty it uses an in-memory database (suitable for tests).
// Blocks must be committed in order, starting from the block after initialRoot.
type Store struct {
	mu         sync.Mutex
	diskdb     ethdb.Database // underlying disk store; closed on Close()
	db         state.Database
	root       common.Hash // current committed state root
	persistent bool
}

// New initializes a TrieStore.
// dbPath=="" uses an in-memory database; dbPath!="" opens (or creates) a PebbleDB at that path.
// initialRoot is the state root of the last committed block; use types.EmptyRootHash for a fresh store.
// Returns an error if the backing store cannot be opened or if initialRoot is not accessible.
func New(dbPath string, initialRoot common.Hash) (*Store, error) {
	var diskdb ethdb.Database
	var persistent bool

	if dbPath == "" {
		diskdb = rawdb.NewMemoryDatabase()
	} else {
		kv, err := pebble.New(dbPath, 128, 64, "fabric-x-evm/trie", false)
		if err != nil {
			return nil, fmt.Errorf("open pebble trie db at %s: %w", dbPath, err)
		}
		diskdb = rawdb.NewDatabase(kv)
		persistent = true
	}

	tdb := triedb.NewDatabase(diskdb, triedb.HashDefaults)
	db := state.NewDatabase(tdb, nil)

	// Validate that initialRoot is accessible in the backing store.
	if _, err := state.New(initialRoot, db); err != nil {
		return nil, fmt.Errorf("trie root %s not found in store: %w", initialRoot, err)
	}

	return &Store{diskdb: diskdb, db: db, root: initialRoot, persistent: persistent}, nil
}

// selfDestructMarkerPrefix flags a same-tx-created account that self-destructed (EIP-6780).
const selfDestructMarkerPrefix = "sd:"

// Commit applies the write sets of all valid transactions in the block to the MPT
// and returns the resulting StateRoot.
func (s *Store) Commit(ctx context.Context, block blocks.Block) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sdb, err := state.New(s.root, s.db)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open state at root %s: %w", s.root, err)
	}

	for _, tx := range block.Transactions {
		if !tx.Valid {
			// Fabric did not apply this transaction's write set.
			continue
		}
		for _, nsRWS := range tx.NsRWS {
			// Self-destruct markers apply after this RWS's other writes: Result()
			// keeps the full write history (Fabric's ledger wants that), so a
			// destroyed address's pre-destruct field writes are still present in
			// the set. Applying the wipe last discards them correctly for the
			// common case (destruct, no recreate). A same-tx recreate after the
			// destruct would be wiped too — a known limitation, not attempted here.
			for _, w := range nsRWS.RWS.Writes {
				if !strings.HasPrefix(w.Key, selfDestructMarkerPrefix) {
					applyWrite(sdb, w)
				}
			}
			for _, w := range nsRWS.RWS.Writes {
				if strings.HasPrefix(w.Key, selfDestructMarkerPrefix) {
					applySelfDestruct(sdb, w)
				}
			}
		}
	}

	// noStorageWiping=false: SelfDestruct (via a self-destruct marker) can delete
	// an account's already-committed storage subtree; geth's Commit rejects that
	// wipe unless explicitly allowed.
	newRoot, err := sdb.Commit(block.Number, true, false)
	if err != nil {
		return common.Hash{}, fmt.Errorf("state commit block %d: %w", block.Number, err)
	}
	if s.persistent {
		if err := s.db.TrieDB().Commit(newRoot, false); err != nil {
			return common.Hash{}, fmt.Errorf("flush trie block %d: %w", block.Number, err)
		}
	}
	s.root = newRoot
	return newRoot, nil
}

// Root returns the current committed state root.
func (s *Store) Root() common.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.root
}

// Close releases underlying trie database resources.
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.TrieDB().Close()
	s.diskdb.Close()
}

// applyWrite interprets a Fabric KVWrite and applies it to the StateDB.
// Keys with unknown prefixes are silently ignored.
func applyWrite(sdb *state.StateDB, w blocks.KVWrite) {
	switch {
	case strings.HasPrefix(w.Key, "acc:"):
		applyAccountWrite(sdb, w)
	case strings.HasPrefix(w.Key, "str:"):
		applyStorageWrite(sdb, w)
	}
}

// applySelfDestruct handles a "sd:<hexAddr>" marker: a same-tx-created account
// self-destructed (EIP-6780). Prior-block storage slots this transaction never
// touched can't be enumerated from the write set, so this delegates to the real
// trie's own SelfDestruct rather than trying to delete matching "str:" keys by hand.
func applySelfDestruct(sdb *state.StateDB, w blocks.KVWrite) {
	addr := common.HexToAddress(strings.TrimPrefix(w.Key, "sd:"))
	sdb.SelfDestruct(addr)
}

// applyAccountWrite handles keys of the form "acc:<hexAddr>:<field>".
// Fields: "bal" (balance), "nonce", "code".
//
// Encoding mirrors endorser/state.go SnapshotDB:
//   - bal:   big.Int.Bytes() — empty slice means zero
//   - nonce: 8-byte big-endian uint64
//   - code:  raw bytecode bytes
func applyAccountWrite(sdb *state.StateDB, w blocks.KVWrite) {
	parts := strings.SplitN(w.Key, ":", 3)
	if len(parts) != 3 {
		return
	}
	addr := common.HexToAddress(parts[1])

	switch parts[2] {
	case "bal":
		if w.IsDelete || len(w.Value) == 0 {
			sdb.SetBalance(addr, new(uint256.Int), tracing.BalanceChangeUnspecified)
		} else {
			bal := new(big.Int).SetBytes(w.Value)
			u, _ := uint256.FromBig(bal)
			sdb.SetBalance(addr, u, tracing.BalanceChangeUnspecified)
		}
	case "nonce":
		if w.IsDelete {
			sdb.SetNonce(addr, 0, tracing.NonceChangeUnspecified)
		} else if len(w.Value) == 8 {
			sdb.SetNonce(addr, binary.BigEndian.Uint64(w.Value), tracing.NonceChangeUnspecified)
		}
	case "code":
		if w.IsDelete {
			sdb.SetCode(addr, nil, tracing.CodeChangeUnspecified)
		} else {
			sdb.SetCode(addr, w.Value, tracing.CodeChangeUnspecified)
		}
	}
}

// applyStorageWrite handles keys of the form "str:<hexAddr>:<hexSlot>".
//
// Encoding mirrors endorser/execution/statedb.go StateDB.SetState:
// the value is the raw 32-byte big-endian value (common.Hash.Bytes()).
func applyStorageWrite(sdb *state.StateDB, w blocks.KVWrite) {
	parts := strings.SplitN(w.Key, ":", 3)
	if len(parts) != 3 {
		return
	}
	addr := common.HexToAddress(parts[1])
	slot := common.HexToHash(parts[2])

	if w.IsDelete {
		sdb.SetState(addr, slot, common.Hash{})
	} else {
		sdb.SetState(addr, slot, common.BytesToHash(w.Value))
	}
}
