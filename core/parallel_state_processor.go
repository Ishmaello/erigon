// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/blockstm"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
)

type ParallelEVMConfig struct {
	Enable               bool
	SpeculativeProcesses int
}

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type ParallelStateProcessor struct {
	config *chain.Config    // Chain configuration options
	engine consensus.Engine // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewParallelStateProcessor(config *chain.Config, engine consensus.Engine) *ParallelStateProcessor {
	return &ParallelStateProcessor{
		config: config,
		engine: engine,
	}
}

type ExecutionTask struct {
	config *chain.Config

	gasLimit                   uint64
	blockHash                  libcommon.Hash
	tx                         types.Transaction
	block                      *types.Block
	index                      int
	statedb                    *state.IntraBlockState // State database that stores the modified values after tx execution.
	cleanStateDB               *state.IntraBlockState // A clean copy of the initial statedb. It should not be modified.
	finalStateDB               *state.IntraBlockState // The final statedb.
	header                     *types.Header
	evmConfig                  *vm.Config
	result                     *ExecutionResult
	shouldDelayFeeCal          *bool
	shouldRerunWithoutFeeDelay bool
	sender                     libcommon.Address
	totalUsedGas               *uint64
	receipts                   *types.Receipts
	allLogs                    *[]*types.Log
	stateWriter                state.StateWriter

	// length of dependencies          -> 2 + k (k = a whole number)
	// first 2 element in dependencies -> transaction index, and flag representing if delay is allowed or not
	//                                       (0 -> delay is not allowed, 1 -> delay is allowed)
	// next k elements in dependencies -> transaction indexes on which transaction i is dependent on
	dependencies []int

	blockContext evmtypes.BlockContext
	evm          vm.VMInterface
	engine       consensus.EngineReader
	msg          *types.Message
	coinbase     libcommon.Address
}

func (task *ExecutionTask) Execute(mvh *blockstm.MVHashMap, incarnation int) (err error) {
	task.statedb = task.cleanStateDB.Copy()
	task.statedb.Prepare(task.tx.Hash(), task.blockHash, task.index)
	task.statedb.SetMVHashmap(mvh)
	task.statedb.SetBlockSTMIncarnation(incarnation)

	rules := task.evm.ChainRules()
	if task.msg != nil {
		msg, err := task.tx.AsMessage(*types.MakeSigner(task.config, task.header.Number.Uint64()), task.header.BaseFee, rules)
		if err != nil {
			return err
		}
		msg.SetCheckNonce(!task.evmConfig.StatelessExec)

		if msg.FeeCap().IsZero() && task.engine != nil {
			// Only zero-gas transactions may be service ones
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return SysCallContract(contract, data, *task.evm.ChainConfig(), task.statedb, task.header, task.engine, true /* constCall */)
			}
			msg.SetIsFree(task.engine.IsServiceTransaction(msg.From(), syscall))
		}

		task.msg = &msg
		task.sender = msg.From()
	}

	task.evm.Reset(task.evm.TxContext(), task.statedb)

	defer func() {
		if r := recover(); r != nil {
			// In some pre-matured executions, EVM will panic. Recover from panic and retry the execution.
			log.Debug("Recovered from EVM failure.", "Error:", r)

			err = blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex()}

			return
		}
	}()

	// Apply the transaction to the current state (included in the env).
	if *task.shouldDelayFeeCal {
		task.result, err = ApplyMessageNoFeeBurnOrTip(task.evm, task.msg, new(GasPool).AddGas(task.gasLimit), true, false)

		if task.result == nil || err != nil {
			return blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		}

		reads := task.statedb.MVReadMap()

		if _, ok := reads[blockstm.NewSubpathKey(task.blockContext.Coinbase, state.BalancePath)]; ok {
			log.Info("Coinbase is in MVReadMap", "address", task.blockContext.Coinbase)

			task.shouldRerunWithoutFeeDelay = true
		}

		if _, ok := reads[blockstm.NewSubpathKey(task.result.BurntContractAddress, state.BalancePath)]; ok {
			log.Info("BurntContractAddress is in MVReadMap", "address", task.result.BurntContractAddress)

			task.shouldRerunWithoutFeeDelay = true
		}
	} else {
		task.result, err = ApplyMessage(task.evm, task.msg, new(GasPool).AddGas(task.gasLimit), true, false)
	}

	if task.statedb.HadInvalidRead() || err != nil {
		err = blockstm.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		return
	}

	task.statedb.FinalizeTx(task.evm.ChainRules(), task.stateWriter)

	return
}

func (task *ExecutionTask) MVReadList() []blockstm.ReadDescriptor {
	return task.statedb.MVReadList()
}

func (task *ExecutionTask) MVWriteList() []blockstm.WriteDescriptor {
	return task.statedb.MVWriteList()
}

func (task *ExecutionTask) MVFullWriteList() []blockstm.WriteDescriptor {
	return task.statedb.MVFullWriteList()
}

func (task *ExecutionTask) Sender() libcommon.Address {
	return task.sender
}

func (task *ExecutionTask) Hash() libcommon.Hash {
	return task.tx.Hash()
}

func (task *ExecutionTask) Dependencies() []int {
	return task.dependencies
}

func (task *ExecutionTask) Settle() {
	defer func() {
		if r := recover(); r != nil {
			// In some rare cases, ApplyMVWriteSet will panic due to an index out of range error when calculating the
			// address hash in sha3 module. Recover from panic and continue the execution.
			// After recovery, block receipts or merckle root will be incorrect, but this is fine, because the block
			// will be rejected and re-synced.
			log.Info("Recovered from error", "Error:", r)
			return
		}
	}()

	task.finalStateDB.Prepare(task.tx.Hash(), task.blockHash, task.index)

	coinbaseBalance := task.finalStateDB.GetBalance(task.coinbase)

	task.finalStateDB.ApplyMVWriteSet(task.statedb.MVWriteList())

	for _, l := range task.statedb.GetLogs(task.tx.Hash()) {
		task.finalStateDB.AddLog(l)
	}

	if *task.shouldDelayFeeCal {
		if task.config.IsLondon(task.block.NumberU64()) {
			task.finalStateDB.AddBalance(task.result.BurntContractAddress, task.result.FeeBurnt)
		}

		task.finalStateDB.AddBalance(task.coinbase, task.result.FeeTipped)
		output1 := task.result.SenderInitBalance
		output2 := coinbaseBalance.Clone()

		// Deprecating transfer log and will be removed in future fork. PLEASE DO NOT USE this transfer log going forward. Parameters won't get updated as expected going forward with EIP1559
		// add transfer log
		AddFeeTransferLog(
			task.finalStateDB,

			task.msg.From(),
			task.coinbase,

			task.result.FeeTipped,
			task.result.SenderInitBalance,
			coinbaseBalance,
			output1.Sub(output1, task.result.FeeTipped),
			output2.Add(output2, task.result.FeeTipped),
		)
	}

	// Update the state with pending changes.
	var root []byte

	if task.config.IsByzantium(task.block.NumberU64()) {
		task.finalStateDB.FinalizeTx(task.evm.ChainRules(), task.stateWriter)
	}

	*task.totalUsedGas += task.result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: task.tx.Type(), PostState: root, CumulativeGasUsed: *task.totalUsedGas}
	if task.result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	receipt.TxHash = task.tx.Hash()
	receipt.GasUsed = task.result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if task.msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(task.msg.From(), task.tx.GetNonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = task.finalStateDB.GetLogs(task.tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = task.blockHash
	receipt.BlockNumber = task.block.Number()
	receipt.TransactionIndex = uint(task.finalStateDB.TxIndex())

	*task.receipts = append(*task.receipts, receipt)
	*task.allLogs = append(*task.allLogs, receipt.Logs...)
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
// nolint:gocognit
func ParallelExecuteBlockEphemerallyBor(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	epochReader consensus.EpochReader,
	chainReader consensus.ChainHeaderReader,
	getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
) (*EphemeralExecResult, error) {

	defer BlockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit())

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, epochReader, block.Header(), block.Transactions(), block.Uncles(), chainConfig, ibs); err != nil {
			return nil, err
		}
	}

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	noop := state.NewNoopWriter()

	shouldDelayFeeCal := true
	tasks := make([]blockstm.ExecTask, 0, len(block.Transactions()))

	var logs []*types.Log

	blockContext := NewEVMBlockContext(header, blockHashFunc, engine, nil)
	vmenv := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, *vmConfig)

	for i, tx := range block.Transactions() {
		cleansdb := ibs.Copy()
		task := &ExecutionTask{
			config:            chainConfig,
			gasLimit:          block.GasLimit(),
			blockHash:         block.Hash(),
			tx:                tx,
			index:             i,
			cleanStateDB:      cleansdb,
			finalStateDB:      ibs,
			header:            header,
			evmConfig:         vmConfig,
			shouldDelayFeeCal: &shouldDelayFeeCal,
			totalUsedGas:      usedGas,
			receipts:          &receipts,
			allLogs:           &logs,
			dependencies:      nil,
			blockContext:      blockContext,
			evm:               vmenv,
			coinbase:          blockContext.Coinbase,
			stateWriter:       noop,
		}

		tasks = append(tasks, task)
	}

	backupStateDB := ibs.Copy()

	_, err := blockstm.ExecuteParallel(tasks, false, false)

	for _, task := range tasks {
		task := task.(*ExecutionTask)
		if task.shouldRerunWithoutFeeDelay {
			shouldDelayFeeCal = false
			*ibs = *backupStateDB

			logs = []*types.Log{}
			receipts = types.Receipts{}
			usedGas = new(uint64)

			for _, t := range tasks {
				t := t.(*ExecutionTask)
				t.finalStateDB = backupStateDB
				t.allLogs = &logs
				t.receipts = &receipts
				t.totalUsedGas = usedGas
			}

			_, err = blockstm.ExecuteParallel(tasks, false, false)

			break
		}
	}

	if err != nil {
		log.Error("blockstm error executing block", "err", err)
		return nil, err
	}

	for _, task := range tasks {
		includedTxs = append(includedTxs, task.(*ExecutionTask).tx)
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), epochReader, chainReader, false); err != nil {
			return nil, err
		}
	}

	blockLogs := ibs.Logs()
	stateSyncReceipt := &types.Receipt{}
	if chainConfig.Consensus == chain.BorConsensus && len(blockLogs) > 0 {
		slices.SortStableFunc(blockLogs, func(i, j *types.Log) bool { return i.Index < j.Index })

		if len(blockLogs) > len(logs) {
			stateSyncReceipt.Logs = blockLogs[len(logs):] // get state-sync logs from `state.Logs()`

			// fill the state sync with the correct information
			types.DeriveFieldsForBorReceipt(stateSyncReceipt, block.Hash(), block.NumberU64(), receipts)
			stateSyncReceipt.Status = types.ReceiptStatusSuccessful
		}
	}

	execRs := &EphemeralExecResult{
		TxRoot:           types.DeriveSha(includedTxs),
		ReceiptRoot:      receiptSha,
		Bloom:            bloom,
		LogsHash:         rlpHash(blockLogs),
		Receipts:         receipts,
		Difficulty:       (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:          math.HexOrDecimal64(*usedGas),
		Rejected:         rejectedTxs,
		StateSyncReceipt: stateSyncReceipt,
	}

	return execRs, nil
}

func GetDeps(txDependency [][]uint64) (map[int][]int, map[int]bool) {
	deps := make(map[int][]int)
	delayMap := make(map[int]bool)

	for i := 0; i <= len(txDependency)-1; i++ {
		idx := int(txDependency[i][0])
		shouldDelay := txDependency[i][1] == 1

		delayMap[idx] = shouldDelay

		deps[idx] = []int{}

		for j := 2; j <= len(txDependency[i])-1; j++ {
			deps[idx] = append(deps[idx], int(txDependency[i][j]))
		}
	}

	return deps, delayMap
}
