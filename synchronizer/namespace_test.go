/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package synchronizer

import (
	"context"
	"errors"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nsTx(id string, namespaces ...string) blocks.Transaction {
	tx := blocks.Transaction{ID: id}
	for _, ns := range namespaces {
		tx.NsRWS = append(tx.NsRWS, blocks.NsReadWriteSet{
			Namespace: ns,
			RWS:       blocks.ReadWriteSet{Writes: []blocks.KVWrite{{Key: ns + "-key"}}},
		})
	}
	return tx
}

func txIDs(b blocks.Block) []string {
	ids := make([]string, 0, len(b.Transactions))
	for _, tx := range b.Transactions {
		ids = append(ids, tx.ID)
	}
	return ids
}

func TestFilterNamespace(t *testing.T) {
	t.Run("own transactions pass through unchanged", func(t *testing.T) {
		b := blocks.Block{Number: 7, Transactions: []blocks.Transaction{nsTx("a", "evm"), nsTx("b", "evm")}}
		got := FilterNamespace(b, "evm")
		assert.Equal(t, b, got)
	})

	t.Run("foreign transactions are dropped", func(t *testing.T) {
		b := blocks.Block{Number: 7, Transactions: []blocks.Transaction{
			nsTx("a", "evm"), nsTx("b", "other"), nsTx("c"), nsTx("d", "evm"),
		}}
		got := FilterNamespace(b, "evm")
		assert.Equal(t, uint64(7), got.Number)
		assert.Equal(t, []string{"a", "d"}, txIDs(got))
		assert.Len(t, b.Transactions, 4, "input block must not be modified")
	})

	t.Run("multi-namespace transaction keeps only our read-write set", func(t *testing.T) {
		b := blocks.Block{Transactions: []blocks.Transaction{nsTx("a", "other", "evm", "third")}}
		got := FilterNamespace(b, "evm")
		require.Len(t, got.Transactions, 1)
		require.Len(t, got.Transactions[0].NsRWS, 1)
		assert.Equal(t, "evm", got.Transactions[0].NsRWS[0].Namespace)
		assert.Len(t, b.Transactions[0].NsRWS, 3, "input transaction must not be modified")
	})

	t.Run("block with only foreign transactions is kept empty", func(t *testing.T) {
		b := blocks.Block{Number: 9, Hash: []byte{1}, Transactions: []blocks.Transaction{nsTx("a", "other")}}
		got := FilterNamespace(b, "evm")
		assert.Equal(t, uint64(9), got.Number)
		assert.Equal(t, []byte{1}, got.Hash)
		assert.Empty(t, got.Transactions)
	})
}

type recordingHandler struct {
	seen []blocks.Block
	err  error
}

func (r *recordingHandler) Handle(_ context.Context, b blocks.Block) error {
	r.seen = append(r.seen, b)
	return r.err
}

func TestNamespaceFilterHandler(t *testing.T) {
	first, second := &recordingHandler{}, &recordingHandler{}
	f := newNamespaceFilter("evm", []blocks.BlockHandler{first, second})

	b := blocks.Block{Number: 3, Transactions: []blocks.Transaction{nsTx("a", "evm"), nsTx("b", "other")}}
	require.NoError(t, f.Handle(context.Background(), b))
	for _, h := range []*recordingHandler{first, second} {
		require.Len(t, h.seen, 1)
		assert.Equal(t, []string{"a"}, txIDs(h.seen[0]))
	}

	// An error stops the chain.
	first.err = errors.New("boom")
	require.Error(t, f.Handle(context.Background(), b))
	assert.Len(t, second.seen, 1)
}
