package consensus

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// NewErc20 creates a new instance of an ERC20 token contract
func NewEntryPoint(address common.Address, backend bind.ContractBackend) (*Contract, error) {
	parsedABI, err := abi.JSON(strings.NewReader(entryPointABI))
	if err != nil {
		return nil, err
	}

	contract := bind.NewBoundContract(address, parsedABI, backend, backend, backend)

	return &Contract{
		address:  address,
		abi:      parsedABI,
		contract: contract,
	}, nil
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

const entryPointABI = `[
    {
      "type": "function",
      "name": "verifyAndCall",
      "inputs": [
        {
          "name": "_targets",
          "type": "address[]",
          "internalType": "address[]"
        },
        {
          "name": "_calldata",
          "type": "bytes[]",
          "internalType": "bytes[]"
        },
        {
          "name": "_signature",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [
        {
          "name": "res",
          "type": "bool[]",
          "internalType": "bool[]"
        }
      ],
      "stateMutability": "nonpayable"
    }
  ]`
