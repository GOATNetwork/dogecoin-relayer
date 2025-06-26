package p2p

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/stretchr/testify/require"
)

func genP2PConfig(listenPort int, privKeyHex string, bootstrapPeers []string) config.P2PConfig {
	return config.P2PConfig{
		Enabled:            true,
		ListenAddr:         fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort),
		BootstrapPeers:     bootstrapPeers,
		EnableMDNS:         false,
		KeyDir:             "",
		ProposerPrivateKey: privKeyHex,
	}
}

func TestP2PNetwork_ThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// generate 3 keys using secp256k1
	priv1, _ := crypto.GenerateKey()
	priv2, _ := crypto.GenerateKey()
	priv3, _ := crypto.GenerateKey()
	privHex1 := fmt.Sprintf("%x", crypto.FromECDSA(priv1))
	privHex2 := fmt.Sprintf("%x", crypto.FromECDSA(priv2))
	privHex3 := fmt.Sprintf("%x", crypto.FromECDSA(priv3))

	// Print public keys and addresses
	pub1 := crypto.FromECDSAPub(&priv1.PublicKey)
	pub2 := crypto.FromECDSAPub(&priv2.PublicKey)
	pub3 := crypto.FromECDSAPub(&priv3.PublicKey)
	addr1 := crypto.PubkeyToAddress(priv1.PublicKey)
	addr2 := crypto.PubkeyToAddress(priv2.PublicKey)
	addr3 := crypto.PubkeyToAddress(priv3.PublicKey)

	t.Logf("Node1 - Private: %s", privHex1)
	t.Logf("Node1 - Public:  %x", pub1)
	t.Logf("Node1 - ETH Addr: %s", addr1.Hex())
	t.Logf("Node2 - Private: %s", privHex2)
	t.Logf("Node2 - Public:  %x", pub2)
	t.Logf("Node2 - ETH Addr: %s", addr2.Hex())
	t.Logf("Node3 - Private: %s", privHex3)
	t.Logf("Node3 - Public:  %x", pub3)
	t.Logf("Node3 - ETH Addr: %s", addr3.Hex())

	// start node1
	cfg1 := genP2PConfig(4771, privHex1, nil)
	node1, err := NewNetwork(ctx, cfg1)
	require.NoError(t, err)

	defer node1.Close()

	node1Addr := node1.host.Addrs()[0].String() + "/p2p/" + node1.host.ID().String()

	// start node2, bootstrap to node1
	cfg2 := genP2PConfig(4772, privHex2, []string{node1Addr})
	node2, err := NewNetwork(ctx, cfg2)
	require.NoError(t, err)
	defer node2.Close()

	node2Addr := node2.host.Addrs()[0].String() + "/p2p/" + node2.host.ID().String()

	// start node3, bootstrap to node1 and node2
	cfg3 := genP2PConfig(4773, privHex3, []string{node1Addr, node2Addr})
	node3, err := NewNetwork(ctx, cfg3)
	require.NoError(t, err)
	defer node3.Close()

	// start network
	require.NoError(t, node1.Start())
	require.NoError(t, node2.Start())
	require.NoError(t, node3.Start())

	// wait for nodes to connect
	time.Sleep(3 * time.Second)

	// check each node can see other nodes
	peers1 := node1.GetPeers()
	peers2 := node2.GetPeers()
	peers3 := node3.GetPeers()
	require.GreaterOrEqual(t, len(peers1), 2)
	require.GreaterOrEqual(t, len(peers2), 2)
	require.GreaterOrEqual(t, len(peers3), 2)

	// check ethereum address conversion and compare with original
	originalAddrs := map[string]string{
		node1.ID().String(): addr1.Hex(),
		node2.ID().String(): addr2.Hex(),
		node3.ID().String(): addr3.Hex(),
	}

	t.Logf("\n=== Conversion Results ===")
	for _, n := range []*Network{node1, node2, node3} {
		for _, pid := range n.GetPeers() {
			pub := n.host.Peerstore().PubKey(pid)
			ethAddr, err := convertLibP2pPubKeyToEthereumAddress(pub)
			require.NoError(t, err)

			originalAddr := originalAddrs[pid.String()]
			isMatch := originalAddr == ethAddr.Hex()
			t.Logf("Peer %s:", pid.String())
			t.Logf("  Original ETH addr:  %s", originalAddr)
			t.Logf("  Converted ETH addr: %s", ethAddr.Hex())
			t.Logf("  Match: %v", isMatch)

			// Assert that conversion matches original
			require.Equal(t, originalAddr, ethAddr.Hex(), "ETH address conversion should match original")
		}
	}
}
