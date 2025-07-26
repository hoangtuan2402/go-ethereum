package txpool

import (
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

type TxHandler struct {
	txChan chan *types.Transaction
	quit   chan struct{}
	wg     sync.WaitGroup
}

func NewTxHandler() *TxHandler {
	return &TxHandler{
		txChan: make(chan *types.Transaction, 100),
		quit:   make(chan struct{}),
	}
}

func (h *TxHandler) Start() {
	h.wg.Add(1)
	go h.processTxWorker()
}

func (h *TxHandler) Stop() {
	close(h.quit)
	h.wg.Wait()
}

func (h *TxHandler) HandleNewTx(tx *types.Transaction) {
	select {
	case h.txChan <- tx:
	case <-h.quit:
		return
	default:
		log.Warn("Transaction handler channel full, dropping tx", "hash", tx.Hash().Hex())
	}
}

func (h *TxHandler) processTxWorker() {
	defer h.wg.Done()

	for {
		select {
		case tx := <-h.txChan:
			h.processTx(tx)
		case <-h.quit:
			return
		}
	}
}

func (h *TxHandler) processTx(tx *types.Transaction) {
	log.Info("Processing new transaction",
		"hash", tx.Hash().Hex(),
		"to", tx.To().Hex(),
		"value", tx.Value().String(),
		"gas", tx.Gas(),
		"gasPrice", tx.GasPrice().String(),
		"nonce", tx.Nonce(),
		"dataLen", len(tx.Data()),
	)
}
