package state

import (
	"bytes"
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateReader = (*PlainStateReader)(nil)

// PlainStateReader reads data from so called "plain state".
// Data in the plain state is stored using un-hashed account/storage items
// as opposed to the "normal" state that uses hashes of merkle paths to store items.
type PlainStateReader struct {
	db kv.Getter
}

func (r *PlainStateReader) GetDB() kv.Getter {
	return r.db
}

func (r *PlainStateReader) SetDB(db kv.Getter) {
	r.db = db
}

func NewPlainStateReader(db kv.Getter) *PlainStateReader {
	return &PlainStateReader{
		db: db,
	}
}

func (r *PlainStateReader) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, err := r.db.GetOne(kv.PlainState, address.Bytes())
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *PlainStateReader) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := r.db.GetOne(kv.PlainState, compositeKey)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *PlainStateReader) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	code, err := r.db.GetOne(kv.Code, codeHash.Bytes())
	if len(code) == 0 {
		return nil, nil
	}
	return code, err
}

func (r *PlainStateReader) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *PlainStateReader) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	b, err := r.db.GetOne(kv.IncarnationMap, address.Bytes())
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(b), nil
}

type ThreadSafePlainStateReader struct {
	db          kv.Getter
	readRequest chan readRequest
}

type readRequest struct {
	address     libcommon.Address
	incarnation uint64
	key         *libcommon.Hash
	codeHash    libcommon.Hash
	result      chan readResult
	requestType int
}

const (
	readRequestTypeData = iota
	readRequestTypeStorage
	readRequestTypeCode
)

type readResult struct {
	data []byte
	err  error
}

func NewThreadSafePlainStateReader(db kv.Getter) *ThreadSafePlainStateReader {
	return &ThreadSafePlainStateReader{
		db:          db,
		readRequest: make(chan readRequest),
	}
}

func (r *ThreadSafePlainStateReader) Start() {
	for req := range r.readRequest {
		var res readResult
		switch req.requestType {
		case readRequestTypeStorage:
			res.data, res.err = r.readAccountStorage(req.address, req.incarnation, req.key)
		case readRequestTypeCode:
			res.data, res.err = r.readAccountCode(req.address, req.incarnation, req.codeHash)
		default:
			res.data, res.err = r.readAccountData(req.address)
		}
		req.result <- res
	}
}

func (r *ThreadSafePlainStateReader) Stop() {
	close(r.readRequest)
}

func (r *ThreadSafePlainStateReader) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	req := readRequest{
		address:     address,
		requestType: readRequestTypeData,
		result:      make(chan readResult),
	}
	r.readRequest <- req
	res := <-req.result
	if res.err != nil {
		return nil, res.err
	}
	if len(res.data) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err := a.DecodeForStorage(res.data); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *ThreadSafePlainStateReader) readAccountData(address libcommon.Address) ([]byte, error) {
	return r.db.GetOne(kv.PlainState, address.Bytes())
}

func (r *ThreadSafePlainStateReader) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	req := readRequest{
		address:     address,
		incarnation: incarnation,
		key:         key,
		requestType: readRequestTypeStorage,
		result:      make(chan readResult),
	}
	r.readRequest <- req
	res := <-req.result
	return res.data, res.err
}

func (r *ThreadSafePlainStateReader) readAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := r.db.GetOne(kv.PlainState, compositeKey)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *ThreadSafePlainStateReader) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	req := readRequest{
		address:     address,
		incarnation: incarnation,
		codeHash:    codeHash,
		requestType: readRequestTypeCode,
		result:      make(chan readResult),
	}
	r.readRequest <- req
	res := <-req.result
	return res.data, res.err
}

func (r *ThreadSafePlainStateReader) readAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	return r.db.GetOne(kv.Code, codeHash.Bytes())
}

func (r *ThreadSafePlainStateReader) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *ThreadSafePlainStateReader) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	b, err := r.db.GetOne(kv.IncarnationMap, address.Bytes())
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(b), nil
}
