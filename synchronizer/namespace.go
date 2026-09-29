/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package synchronizer

import (
	"context"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// namespaceFilter is a BlockHandler that hides other applications sharing the
// channel: it forwards each block to handlers with only the transactions that
// touch namespace, each trimmed to its namespace read-write set. Blocks are
// always forwarded (possibly empty) so block numbering stays aligned with the ledger.
type namespaceFilter struct {
	namespace string
	handlers  []blocks.BlockHandler
}

func newNamespaceFilter(namespace string, handlers []blocks.BlockHandler) *namespaceFilter {
	return &namespaceFilter{namespace: namespace, handlers: handlers}
}

// Handle implements blocks.BlockHandler.
func (f *namespaceFilter) Handle(ctx context.Context, b blocks.Block) error {
	b = FilterNamespace(b, f.namespace)
	for _, h := range f.handlers {
		if err := h.Handle(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// FilterNamespace returns b with only the transactions that have a read-write set
// in namespace, keeping only that read-write set. b itself is not modified.
func FilterNamespace(b blocks.Block, namespace string) blocks.Block {
	if onlyNamespace(b.Transactions, namespace) {
		return b // common case: nothing to strip
	}

	txs := make([]blocks.Transaction, 0, len(b.Transactions))
	for _, tx := range b.Transactions {
		for _, rws := range tx.NsRWS {
			if rws.Namespace == namespace {
				tx.NsRWS = []blocks.NsReadWriteSet{rws}
				txs = append(txs, tx)
				break
			}
		}
	}
	b.Transactions = txs
	return b
}

// onlyNamespace reports whether every transaction touches namespace and nothing else.
func onlyNamespace(txs []blocks.Transaction, namespace string) bool {
	for _, tx := range txs {
		if len(tx.NsRWS) != 1 || tx.NsRWS[0].Namespace != namespace {
			return false
		}
	}
	return true
}
