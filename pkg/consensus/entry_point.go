package consensus

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

type EntryPoint struct {
	address  common.Address
	abi      abi.ABI
	contract *bind.BoundContract
}

// NewErc20 creates a new instance of an ERC20 token contract
func NewEntryPoint(address common.Address, backend bind.ContractBackend) (*EntryPoint, error) {
	parsedABI, err := abi.JSON(strings.NewReader(EntryPointABI))
	if err != nil {
		return nil, err
	}

	contract := bind.NewBoundContract(address, parsedABI, backend, backend, backend)

	return &EntryPoint{
		address:  address,
		abi:      parsedABI,
		contract: contract,
	}, nil
}

func (entryPoint *EntryPoint) GenerateVerifyAndCallData(targets []common.Address, calldata []byte, signature []byte) ([]byte, error) {
	return entryPoint.abi.Pack("verifyAndCall", targets, calldata, signature)
}

func (entryPoint *EntryPoint) GenerateChooseNewSubmitterData(signature []byte) ([]byte, error) {
	return entryPoint.abi.Pack("chooseNewSubmitter", signature)
}

func (entryPoint *EntryPoint) GenerateSetProposersData(newProposers []common.Address, signature []byte) ([]byte, error) {
	return entryPoint.abi.Pack("setProposers", newProposers, signature)
}

func (entryPoint *EntryPoint) GenerateSetSignerAddressData(newSigner common.Address, signature []byte) ([]byte, error) {
	return entryPoint.abi.Pack("setSignerAddress", newSigner, signature)
}

func (entryPoint *EntryPoint) GenerateSetStakeThresholdData(newThreshold *big.Int, signature []byte) ([]byte, error) {
	return entryPoint.abi.Pack("setStakeThreshold", newThreshold, signature)
}

func (entryPoint *EntryPoint) GenerateStakeData(amount *big.Int) ([]byte, error) {
	return entryPoint.abi.Pack("stake", amount)
}

func (entryPoint *EntryPoint) GenerateUnstakeData(amount *big.Int) ([]byte, error) {
	return entryPoint.abi.Pack("unstake", amount)
}

const EntryPointABI = `[
    {
      "type": "constructor",
      "inputs": [
        {
          "name": "_stakeToken",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "FORCE_ROTATION_WINDOW",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "MIN_PARTICIPANT_COUNT",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "chooseNewSubmitter",
      "inputs": [
        {
          "name": "_uncompletedTaskCount",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "_signature",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "initialize",
      "inputs": [
        {
          "name": "_tssSigner",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "_initialProposers",
          "type": "address[]",
          "internalType": "address[]"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "isProposer",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "lastSubmissionTime",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "nextSubmitter",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "proposers",
      "inputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "setProposers",
      "inputs": [
        {
          "name": "_newProposers",
          "type": "address[]",
          "internalType": "address[]"
        },
        {
          "name": "_signature",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "setSignerAddress",
      "inputs": [
        {
          "name": "_newSigner",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "_signature",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "setStakeThreshold",
      "inputs": [
        {
          "name": "_newThreshold",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "_signature",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "stake",
      "inputs": [
        {
          "name": "_amount",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "stakeThreshold",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "stakeToken",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "stakedAmounts",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "tssNonce",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "tssSigner",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "unstake",
      "inputs": [
        {
          "name": "_amount",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
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
    },
    {
      "type": "event",
      "name": "Initialized",
      "inputs": [
        {
          "name": "version",
          "type": "uint64",
          "indexed": false,
          "internalType": "uint64"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "SetProposer",
      "inputs": [
        {
          "name": "participants",
          "type": "address[]",
          "indexed": false,
          "internalType": "address[]"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "SetSigner",
      "inputs": [
        {
          "name": "newSigner",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "Stake",
      "inputs": [
        {
          "name": "staker",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        },
        {
          "name": "amount",
          "type": "uint256",
          "indexed": false,
          "internalType": "uint256"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "StakeThresholdUpdated",
      "inputs": [
        {
          "name": "newThreshold",
          "type": "uint256",
          "indexed": false,
          "internalType": "uint256"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "SubmitterChosen",
      "inputs": [
        {
          "name": "newSubmitter",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "SubmitterRotationRequested",
      "inputs": [
        {
          "name": "requester",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        },
        {
          "name": "currentSubmitter",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        }
      ],
      "anonymous": false
    },
    {
      "type": "event",
      "name": "Unstake",
      "inputs": [
        {
          "name": "staker",
          "type": "address",
          "indexed": true,
          "internalType": "address"
        },
        {
          "name": "amount",
          "type": "uint256",
          "indexed": false,
          "internalType": "uint256"
        }
      ],
      "anonymous": false
    },
    {
      "type": "error",
      "name": "IncorrectSubmitter",
      "inputs": [
        {
          "name": "sender",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "submitter",
          "type": "address",
          "internalType": "address"
        }
      ]
    },
    {
      "type": "error",
      "name": "InvalidInitialization",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotInitializing",
      "inputs": []
    },
    {
      "type": "error",
      "name": "ReentrancyGuardReentrantCall",
      "inputs": []
    },
    {
      "type": "error",
      "name": "RotationWindowNotPassed",
      "inputs": [
        {
          "name": "current",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "window",
          "type": "uint256",
          "internalType": "uint256"
        }
      ]
    }
  ]`
