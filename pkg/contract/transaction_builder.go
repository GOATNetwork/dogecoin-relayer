package contract

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// BridgeTransaction represents the Go equivalent of IDogechain.BridgeTransaction
type BridgeTransaction struct {
	DestEvmAddress common.Address
	Amount         *big.Int
	TxBytes        []byte
}

// NewEntryPoint creates a new instance of an EntryPoint contract
func NewEntryPoint(address common.Address, backend bind.ContractBackend, abiFilePath string) (*Contract, error) {
	parsedABI, err := LoadABIFromFile(abiFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load ABI for EntryPoint contract: %w", err)
	}

	contract := bind.NewBoundContract(address, parsedABI, backend, backend, backend)

	return &Contract{
		address:  address,
		abi:      parsedABI,
		contract: contract,
	}, nil
}

// NewEntryPointWithABI creates a new instance of an EntryPoint contract with provided ABI
func NewEntryPointWithABI(address common.Address, backend bind.ContractBackend, contractABI abi.ABI) (*Contract, error) {
	contract := bind.NewBoundContract(address, contractABI, backend, backend, backend)

	return &Contract{
		address:  address,
		abi:      contractABI,
		contract: contract,
	}, nil
}

func (contract *Contract) GetCurrentProposer() (common.Address, error) {
	var out []interface{}
	err := contract.contract.Call(&bind.CallOpts{}, &out, "nextSubmitter")
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to get current proposer: %w", err)
	}

	if len(out) == 0 {
		return common.Address{}, fmt.Errorf("no result returned from nextSubmitter call")
	}

	proposer, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("failed to convert result to address")
	}

	return proposer, nil
}

// GenerateBridgeInTxData generates the transaction data for the bridgeIn function call
func (contract *Contract) GenerateBridgeInTxData(bridgeTxs []BridgeTransaction) ([]byte, error) {
	// Convert the bridgeTxs to the format expected by the ABI
	// The bridgeIn function expects: bridgeIn(IDogechain.BridgeTransaction[] memory bridgeTxs)

	// Create the array type for BridgeTransaction[]
	bridgeTransactionArrayType, err := abi.NewType("tuple[]", "struct BridgeTransaction[]", []abi.ArgumentMarshaling{
		{Name: "destEvmAddress", Type: "address"},
		{Name: "amount", Type: "uint256"},
		{Name: "txBytes", Type: "bytes"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create BridgeTransaction array type: %w", err)
	}

	// Convert Go struct to ABI-compatible format
	bridgeTransactionStructs := make([]struct {
		DestEvmAddress common.Address
		Amount         *big.Int
		TxBytes        []byte
	}, len(bridgeTxs))

	for i, bridgeTx := range bridgeTxs {
		bridgeTransactionStructs[i] = struct {
			DestEvmAddress common.Address
			Amount         *big.Int
			TxBytes        []byte
		}{
			DestEvmAddress: bridgeTx.DestEvmAddress,
			Amount:         bridgeTx.Amount,
			TxBytes:        bridgeTx.TxBytes,
		}
	}

	// Create arguments for the bridgeIn function
	arguments := abi.Arguments{
		{Type: bridgeTransactionArrayType, Name: "bridgeTxs"},
	}

	// Pack the arguments
	calldata, err := arguments.Pack(bridgeTransactionStructs)
	if err != nil {
		return nil, fmt.Errorf("failed to pack bridgeIn arguments: %w", err)
	}

	// Get the function selector for bridgeIn
	bridgeInSelector := crypto.Keccak256([]byte("bridgeIn((address,uint256,bytes)[])"))[:4]

	// Combine function selector with calldata
	txData := append(bridgeInSelector, calldata...)

	return txData, nil
}

func (contract *Contract) GenerateBridgeOutFinishTxData(batchId *big.Int, bridgeTx BridgeTransaction, taskIds []*big.Int) ([]byte, error) {
	// The bridgeOutFinish function expects: bridgeOutFinish(uint256 batchId, IDogechain.BridgeTransaction memory bridgeTx, uint256[] memory taskIds)

	// Create the tuple type for BridgeTransaction struct
	bridgeTransactionType, err := abi.NewType("tuple", "struct BridgeTransaction", []abi.ArgumentMarshaling{
		{Name: "destEvmAddress", Type: "address"},
		{Name: "amount", Type: "uint256"},
		{Name: "txBytes", Type: "bytes"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create BridgeTransaction type: %w", err)
	}

	// Create the uint256 array type for taskIds
	uint256ArrayType, err := abi.NewType("uint256[]", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create uint256[] type: %w", err)
	}

	// Convert Go struct to ABI-compatible format
	bridgeTransactionStruct := struct {
		DestEvmAddress common.Address
		Amount         *big.Int
		TxBytes        []byte
	}{
		DestEvmAddress: bridgeTx.DestEvmAddress,
		Amount:         bridgeTx.Amount,
		TxBytes:        bridgeTx.TxBytes,
	}

	// Create arguments for the bridgeOutFinish function
	arguments := abi.Arguments{
		{Type: Uint256Type, Name: "batchId"},
		{Type: bridgeTransactionType, Name: "bridgeTx"},
		{Type: uint256ArrayType, Name: "taskIds"},
	}

	// Pack the arguments
	calldata, err := arguments.Pack(batchId, bridgeTransactionStruct, taskIds)
	if err != nil {
		return nil, fmt.Errorf("failed to pack bridgeOutFinish arguments: %w", err)
	}

	// Get the function selector for bridgeOutFinish
	bridgeOutFinishSelector := crypto.Keccak256([]byte("bridgeOutFinish(uint256,(address,uint256,bytes),uint256[])"))[:4]

	// Combine function selector with calldata
	txData := append(bridgeOutFinishSelector, calldata...)

	return txData, nil
}

// GenerateVerifyAndCallTxData generates the complete transaction data with signature
func (contract *Contract) GenerateVerifyAndCallTxData(targets []common.Address, calldata [][]byte, signature []byte) ([]byte, error) {
	return contract.abi.Pack("verifyAndCall", targets, calldata, signature)
}

// CreateVerifyAndCallHash creates just the hash that should be signed for the verifyAndCall function
// This matches: keccak256(abi.encode(_targets, _calldata, tssNonce, block.chainid))
func CreateVerifyAndCallHash(targets []common.Address, calldata [][]byte, tssNonce *big.Int, chainID *big.Int) ([32]byte, error) {
	// Create the arguments for ABI encoding using global types
	arguments := abi.Arguments{
		{Type: AddressArrayType},
		{Type: BytesArrayType},
		{Type: Uint256Type},
		{Type: Uint256Type},
	}

	// Encode the parameters
	encoded, err := arguments.Pack(targets, calldata, tssNonce, chainID)
	if err != nil {
		return [32]byte{}, err
	}

	// Hash the encoded data
	hash := crypto.Keccak256Hash(encoded)
	return hash, nil
}

// TODO: Implement the other functions
