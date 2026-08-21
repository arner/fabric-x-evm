/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-evm/integration/contracts"
)

// switchWatchingLogger wraps TestLogger and additionally closes switched the
// first time it observes hybridx's "switching to notification" log line, so
// tests can wait on the switch without reaching into HybridSynchronizer's
// unexported state.
type switchWatchingLogger struct {
	TestLogger
	once     sync.Once
	switched chan struct{}
}

func newSwitchWatchingLogger(t *testing.T) *switchWatchingLogger {
	return &switchWatchingLogger{
		TestLogger: TestLogger{T: t},
		switched:   make(chan struct{}),
	}
}

func (l *switchWatchingLogger) Infof(format string, v ...any) {
	l.TestLogger.Infof(format, v...)
	if strings.HasPrefix(format, "hybridx: switching to notification") {
		l.once.Do(func() { close(l.switched) })
	}
}

// testHybridSwitchesToNotification verifies, against a real fabric-x committer,
// that the hybrid synchronizer actually switches from delivery to notification
// mode under continuous traffic — the scenario that was silently broken before
// the nDel+1 >= batch.BlockNumber fix in gateway/app/hybridx/hybrid.go
// (previously nDel could at best reach batch.BlockNumber-1 under continuous
// traffic, so the switch never fired: no error, no log, permanent delivery-only
// fallback). It also submits a transaction after the switch and confirms it is
// correctly reflected on-chain, exercising the notification path's
// double-dispatch guard and block-hash placeholder end-to-end against the real
// committer rather than the fabrictest fake the unit tests use.
func testHybridSwitchesToNotification(t *testing.T) {
	logs := newSwitchWatchingLogger(t)
	th, err := newFileConfigHarness(t, logs, evmConfig(""), "", "fabx.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { th.Stop() })

	node := th.Gateways[0]
	ethClient, err := NewEthClient(contracts.CounterMetaData, th.ethChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Continuous traffic: fire a deploy and several calls back-to-back with no
	// delay, so each new block's notification batch races the delivery
	// synchronizer's processing of that same block — the exact condition that
	// never switched before the fix.
	addr := deploySmartContract(t, node, ethClient)
	const calls = 20
	for range calls {
		callSmartContract(t, ethClient, addr, node, "increment")
	}

	select {
	case <-logs.switched:
		t.Log("hybridx switched to notification mode")
	case <-time.After(10 * time.Second):
		t.Fatal("hybridx never switched to notification mode under continuous traffic")
	}

	// The synchronizer must keep working correctly after the switch.
	callSmartContract(t, ethClient, addr, node, "increment")
	querySmartContractExpect(t, ethClient, addr, th, big.NewInt(calls+1), "getCount")
}
