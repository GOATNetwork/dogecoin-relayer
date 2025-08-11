# Dogecoin Relayer Consensus Model

## Overview
The consensus module is responsible for managing the on-chain contract process of the Dogecoin bridge on goatnetwork. It is responsible for:

- Detecting the on-chain contract events of the Dogecoin bridge on goatnetwork, such as the `BridgeIn`, `BridgeOutProposed` and `BridgeOutFinished` events.
- Detecting the on-chain contract events of the decentralized relayer, such as the `ProposerSelected`, `AddProposerReqeusted`, `RemoveProposerReqeusted` and `ProposerConfirmed` events.
- Submitting the deposit and withdraw transactions to the goatnetwork to finish the on-chain contract process, call the following functions: `bridgeIn()`, `bridgeOutFinish()`. This process is from data status of doge module.


## Module entrance

- `event_manager`
- `utxo_processor`
- `proposer_manager`

## Data Structure 

The consensus module uses the following data structures to manage the on-chain contract process of the Dogecoin bridge on goatnetwork:

- `Deposit` data refer to the `UTXO` data:
    - `id`: PK
    - `txid`: The transaction hash of the deposit (UK).
    - `vout`: The output index of the deposit (UK).
    - `address`: The address of the sender.
    - `amount`: The amount of the deposit.
    - `tx_bytes`: The transaction bytes of the deposit.
    - `status`: The status of the deposit, `init`, `pending`, `confirmed`, `failed`.
    - `evm_tx_hash`
    - `evm_block`
    - `evm_log_index`
    - `create_time`
    - `update_time`

- `Withdraw` data:
    - `id`: PK
    - `req_task_id`: The task id of the withdraw request (UK).
    - `req_tx_hash`: The transaction hash of the withdraw request.
    - `req_block`
    - `req_log_index`
    - `status`: The status of the withdraw request, `init`, `pending1`, `utxo_out`, `pending2`, `confirmed`, `failed`.
    - `txid`
    - `vout`
    - `tx_bytes`
    - `finish_tx_hash`: The transaction hash of the withdraw finish.
    - `finish_block`: The block height of the withdraw finish, from event `BridgeOutFinished`.
    - `finish_log_index`: The log index of the withdraw finish, from event `BridgeOutFinished`.
    - `create_time`
    - `update_time`

- `Proposer` data:
    - `id`: PK
    - `address`: The address of the proposer (UK).
    - `status`: The status of the proposer, `pending`, `ok`, `exit`.
    - `pending_event`
    - `join_block`
    - `exit_block`
    - `create_time`
    - `update_time`
