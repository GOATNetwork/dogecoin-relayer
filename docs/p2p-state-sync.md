# P2P State Synchronization Architecture

## Overview

This document describes how state changes are synchronized across relayer nodes. There are two categories of state transitions:

1. **On-chain consensus** - State changes triggered by blockchain events (automatically synchronized via chain scanning)
2. **Off-chain state** - State changes that happen locally and need P2P broadcast for synchronization

## Database Tables with Status Fields

| Table | Status Field | Status Values | Sync Method |
|-------|--------------|---------------|-------------|
| `deposits` | `status` | pending, confirmed | On-chain (BridgeIn event) |
| `withdrawals` | `status` | create, aggregating, init, pending, confirmed, processed | Mixed |
| `utxos` | `status` | unconfirmed, confirmed, processed, pending, spent | Mixed |
| `send_orders` | `status` | aggregating, init, pending, confirmed, processed, closed | Off-chain |
| `vins` | `status` | (mirrors send_order) | Off-chain |
| `vouts` | `status` | (mirrors send_order) | Off-chain |
| `pending_batches` | `status` | pending, completed, failed | Off-chain |

## State Transition Flows

### 1. Deposit Flow (Dogecoin → EVM)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DEPOSIT FLOW                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [gRPC Deposit Request]                                             │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────┐     P2P: deposit-notification                     │
│  │ Create UTXO  │ ────────────────────────────► Other Nodes         │
│  │ (confirmed)  │                                                    │
│  │ Create Deposit│                                                   │
│  │ (pending)    │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (Proposer only)                                           │
│  ┌──────────────┐     P2P: deposit-proposal                         │
│  │ Create Batch │ ────────────────────────────► Other Nodes         │
│  │ pending_batch│     (TSS signing request)                         │
│  │ (pending)    │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (TSS Success)                                              │
│  ┌──────────────┐                                                    │
│  │ Submit to EVM│ ──► Emits BridgeIn event                          │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (All nodes scan chain)                                    │
│  ┌──────────────┐     ON-CHAIN SYNC                                 │
│  │ Update Deposit│ ◄── BridgeIn event                               │
│  │ (confirmed)  │                                                    │
│  │ Update UTXO  │                                                    │
│  │ (processed)  │                                                    │
│  │ pending_batch│                                                    │
│  │ (completed)  │                                                    │
│  └──────────────┘                                                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2. Withdrawal Flow (EVM → Dogecoin)

```
┌─────────────────────────────────────────────────────────────────────┐
│                       WITHDRAWAL FLOW                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [EVM BridgeOutProposed event]                                      │
│         │                                                            │
│         ▼ (All nodes scan chain)                                    │
│  ┌──────────────┐     ON-CHAIN SYNC                                 │
│  │Create Withdrawal│                                                │
│  │ (create)     │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (Proposer aggregates)                                     │
│  ┌──────────────┐     P2P: bridge-out                               │
│  │ Select UTXOs │ ────────────────────────────► Other Nodes         │
│  │ UTXO→pending │     (TSS signing request)                         │
│  │ Create Batch │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (TSS Success - Fireblocks)                                │
│  ┌──────────────┐     P2P: send-order-broadcasted                   │
│  │Create SendOrder│───────────────────────────► Other Nodes         │
│  │ VINs, VOUTs  │                                                    │
│  │ (pending)    │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (Fireblocks signs, txid changes)                          │
│  ┌──────────────┐     P2P: send-order-txid-update                   │
│  │Update SendOrder│──────────────────────────► Other Nodes          │
│  │ (new txid)   │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (Broadcast to Dogecoin)                                   │
│  ┌──────────────┐                                                    │
│  │ Update UTXOs │ ──► Dogecoin mempool/chain                        │
│  │ (spent)      │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (Dogecoin confirmation)                                   │
│  ┌──────────────┐     ON-CHAIN SYNC (Dogecoin)                      │
│  │Update SendOrder│ ◄── Dogecoin block scan                         │
│  │ (confirmed)  │                                                    │
│  └──────┬───────┘                                                    │
│         │                                                            │
│         ▼ (EVM confirmation)                                        │
│  ┌──────────────┐     ON-CHAIN SYNC (EVM)                           │
│  │Update Withdrawal│◄── BridgeOutFinished event                     │
│  │ (processed)  │                                                    │
│  └──────────────┘     P2P: withdrawal-status                        │
│                   ────────────────────────────► Other Nodes         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Current P2P Message Types

| Message Type | Constant | Purpose | Trigger |
|--------------|----------|---------|---------|
| `heartbeat` | `P2PMessageTypeHeartbeat` | Network connectivity | Automatic (30s) |
| `deposit-notification` | `P2PMessageTypeDepositNotification` | New deposit via gRPC | gRPC request |
| `deposit-proposal` | `P2PMessageTypeDepositProposal` | Batch deposit for TSS | Proposer creates batch |
| `bridge-out` | `P2PMessageTypeBridgeOut` | Withdrawal proposal for TSS | Proposer creates batch |
| `withdrawal-status` | `P2PMessageTypeWithdrawalStatus` | Withdrawal status sync | Status change |
| `send-order-broadcasted` | `P2PMessageTypeSendOrderBroadcasted` | SendOrder created | After Fireblocks submit |
| `send-order-txid-update` | `P2PMessageTypeSendOrderTxidUpdate` | SendOrder txid changed | After Fireblocks signing |

## Missing P2P Broadcasts (Identified Issues)

### Issue 1: UTXO Status Updates Not Broadcasted

**Problem**: When UTXOs are marked as `pending` (selected for withdrawal) or reset to `processed` (on failure), other nodes don't know.

**Affected Functions**:
- `withdrawal_processor.go:1231` - UTXO marked as pending
- `withdrawal_processor.go:1299` - UTXO marked as pending
- `withdrawal_processor.go:1428` - UTXO reset to processed
- `withdrawal_processor.go:1555` - UTXO reset to processed
- `withdraw_state.go:221` - UTXO reset to processed (CleanProcessingSendOrders)

**Solution**: Add new P2P message type `utxo-status-update` to broadcast UTXO status changes.

### Issue 2: SendOrder Close Not Broadcasted

**Problem**: When `CloseSendOrdersForWithdrawal()` is called, the SendOrder/VINs/VOUTs are closed but other nodes don't know.

**Affected Functions**:
- `withdraw_state.go:149-191` - CloseSendOrdersForWithdrawal

**Solution**: Add new P2P message type `send-order-status-update` or extend existing `send-order-broadcasted` to include status updates.

### Issue 3: pending_batch Completion Not Broadcasted

**Problem**: When `UpdatePendingBatchStatus()` marks a batch as `completed`, other nodes may have stale data.

**Affected Functions**:
- `utxo_processor.go:497` - removePendingSession marks as completed

**Solution**: Add new P2P message type `pending-batch-status` to sync batch status.

### Issue 4: Startup Cleanup Not Broadcasted

**Problem**: `CleanProcessingSendOrders()` resets UTXOs and closes orders on startup without P2P notification.

**Affected Functions**:
- `withdraw_state.go:193-256` - CleanProcessingSendOrders

**Solution**: Either:
1. Broadcast cleanup results via P2P, OR
2. Make cleanup idempotent across all nodes (each node cleans its own state)

## Recommended New P2P Message Types

### 1. `utxo-status-update`
```go
type UTXOStatusUpdatePayload struct {
    Txid      string `json:"txid"`
    OutIndex  int    `json:"out_index"`
    OldStatus string `json:"old_status"`
    NewStatus string `json:"new_status"`
    Reason    string `json:"reason"` // "selected_for_withdrawal", "withdrawal_failed", "cleanup"
    UpdatedAt int64  `json:"updated_at"`
}
```

### 2. `send-order-status-update`
```go
type SendOrderStatusUpdatePayload struct {
    OrderId   string `json:"order_id"`
    Txid      string `json:"txid"`
    OldStatus string `json:"old_status"`
    NewStatus string `json:"new_status"`
    Reason    string `json:"reason"` // "withdrawal_completed", "cleanup", "failure"
    UpdatedAt int64  `json:"updated_at"`
}
```

### 3. `pending-batch-status`
```go
type PendingBatchStatusPayload struct {
    BaseSessionID string `json:"base_session_id"`
    BatchType     string `json:"batch_type"`
    Status        string `json:"status"`
    UpdatedAt     int64  `json:"updated_at"`
}
```

## Implementation Priority

| Priority | Issue | Impact | Complexity |
|----------|-------|--------|------------|
| **HIGH** | UTXO status updates | Causes UTXO selection conflicts | Medium |
| **HIGH** | SendOrder close | Causes orphan orders | Low |
| **MEDIUM** | pending_batch status | Causes retry confusion | Low |
| **LOW** | Startup cleanup | Only affects restarts | Medium |

## Status Hierarchy Rules

### UTXO Status Hierarchy
```
unconfirmed (0) < confirmed (10) < processed (20) < pending (30) < spent (100)
```
- Status can only move to equal or higher rank
- `spent` is immutable (rank 100)
- P2P updates should respect hierarchy (ignore downgrade attempts)

### Withdrawal Status Hierarchy
```
create (0) < aggregating (10) < init (20) < pending (30) < confirmed (40) < processed (50)
```
- Similar hierarchy rules apply
- `processed` is final state

## Files to Modify

| File | Changes |
|------|---------|
| `pkg/types/p2p.go` | Add new message types and payload structs |
| `internal/consensus/withdrawal_processor.go` | Add UTXO status broadcasts |
| `internal/wallet/withdraw_state.go` | Add SendOrder status broadcasts |
| `internal/consensus/utxo_processor.go` | Add pending_batch status broadcasts |
| `internal/p2p/sender.go` | Add new broadcast helper functions |

## Testing Checklist

- [ ] UTXO status sync when withdrawal is initiated
- [ ] UTXO status sync when withdrawal fails and UTXOs reset
- [ ] SendOrder status sync when orders are closed
- [ ] pending_batch status sync across nodes
- [ ] Verify status hierarchy is respected in P2P handlers
- [ ] Test node restart synchronization
