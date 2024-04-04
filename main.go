package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/cockroachdb/errors"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/multiformats/go-multiaddr"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var logger = log.Logger("main")

type Position struct {
	Row int `json:"row"`
	Col int `json:"col"`
}

type Mapping struct {
	gorm.Model
	BlockNumber uint64
	BlockHash   string
	Cid         string
}

type V3Block struct {
	Block struct {
		Header struct {
			Extension struct {
				V3 struct {
					Commitment struct {
						Rows     int        `json:"rows"`
						Cols     int        `json:"cols"`
						DataRoot types.Hash `json:"dataRoot"`
					}
				} `json:"V3"`
			} `json:"extension"`
		} `json:"header"`
	} `json:"block"`
}

func getDataFromDHT(ctx context.Context, kdht *dht.IpfsDHT, blockNumber int) ([][]byte, error) {
	// 256 rows * 2 extension factor
	for row := 0; row < 512; row++ {
		logger.Infof("getting value for %d:%d", blockNumber, row)
		result, err := kdht.GetValue(ctx, fmt.Sprintf("%d:%d", blockNumber, row))
		if err != nil {
			logger.Errorf("failed to get value: %+v", err)
		} else {
			logger.Infof("got value for %d:%d - len: %d", blockNumber, row, len(result))
		}
	}
	return nil, nil
}

func getDataFromRPC(ctx context.Context, api *gsrpc.SubstrateAPI, block V3Block, blockHash types.Hash) ([][]byte, error) {
	var positions []Position
	rows := block.Block.Header.Extension.V3.Commitment.Rows
	cols := block.Block.Header.Extension.V3.Commitment.Cols
	for row := 0; row < rows; row++ {
		for col := 0; col < cols; col++ {
			positions = append(positions, Position{row, col})
		}
	}

	for start := 0; start < len(positions); start += 64 {
		end := start + 64
		if end > len(positions) {
			end = len(positions)
		}
		logger.Infof("getting proof for %s [%d, %d)", blockHash.Hex(), start, end)
		var result []byte
		err := api.Client.Call(&result, "kate_queryProof", positions[start:end], blockHash)
		if err != nil {
			logger.Errorf("error: %v", err)
		} else {
			logger.Infof("result length: %d", len(result))
		}
	}

	return nil, nil
}

func start() error {
	/*
		host, err := InitHost()
		if err != nil {
			return errors.WithStack(err)
		}
		kdht, err := InitDHT(ctx, host)
		if err != nil {
			return errors.WithStack(err)
		}
	*/
	db, err := gorm.Open(sqlite.Open("mapping.db"), &gorm.Config{})
	if err != nil {
		return errors.WithStack(err)
	}
	err = db.AutoMigrate(&Mapping{})
	if err != nil {
		return errors.WithStack(err)

	}
	api, err := gsrpc.NewSubstrateAPI("wss://goldberg.avail.tools:443/ws")
	if err != nil {
		return errors.WithStack(err)
	}
	subscription, err := api.RPC.Chain.SubscribeFinalizedHeads()
	if err != nil {
		return errors.WithStack(err)
	}

	for header := range subscription.Chan() {
		logger.Infof("Chain is at block: #%d", header.Number)
		blockHash, err := api.RPC.Chain.GetBlockHash(uint64(header.Number))
		if err != nil {
			return errors.WithStack(err)
		}
		logger.Infof("block hash - %s", blockHash.Hex())
		var block interface{}
		err = api.Client.Call(&block, "chain_getBlock", blockHash)
		if err != nil {
			return errors.WithStack(err)
		}

		/*
			node, err := cborutil.AsIpld(block)
			if err != nil {
				return errors.WithStack(err)
			}
		*/
		str, err := json.Marshal(block)
		if err != nil {
			return errors.WithStack(err)
		}

		nodeCid := cid.NewCidV1(cid.DagJSON, u.Hash([]byte(str)))

		buffer := new(bytes.Buffer)
		carHeader := car.CarHeader{
			Roots:   []cid.Cid{nodeCid},
			Version: 1,
		}
		err = car.WriteHeader(&carHeader, buffer)
		if err != nil {
			return errors.WithStack(err)
		}

		err = util.LdWrite(buffer, nodeCid.Bytes(), []byte(str))
		if err != nil {
			return errors.WithStack(err)
		}

		filename := fmt.Sprintf("%d.car", header.Number)
		err = os.WriteFile(filename, buffer.Bytes(), 0644)
		if err != nil {
			return errors.WithStack(err)
		}

		db.Create(&Mapping{
			BlockNumber: uint64(header.Number),
			BlockHash:   blockHash.Hex(),
			Cid:         nodeCid.String(),
		})

		cmd := exec.Command("w3", "up", "--car", filename)
		_, err = cmd.Output()
		if err != nil {
			return errors.WithStack(err)
		}
		//fmt.Println(string(stdout))
		fmt.Printf("block: %d, hash: %s, cid: %s\n", header.Number, blockHash.Hex(), nodeCid.String())
	}

	subscription.Unsubscribe()
	return nil
}

const yamuxID = "/yamux/1.0.0"

const protocolID = "/avail_kad/id/1.0.0-6f0996"

func InitDHT(ctx context.Context, host host.Host) (*dht.IpfsDHT, error) {
	var boostrappers []peer.AddrInfo
	for _, peerAddr := range []string{"/dns/bootnode.1.lightclient.goldberg.avail.tools/tcp/37000/p2p/12D3KooWBkLsNGaD3SpMaRWtAmWVuiZg1afdNSPbtJ8M8r9ArGRT"} {
		//for _, peerAddr := range []string{"/ip4/127.0.0.1/tcp/37000/p2p/12D3KooWPRvdxAyaoq6hqBxCZkFknNW7k4aogJtQqWrQENmBEZWx"} {
		ma, err := multiaddr.NewMultiaddr(peerAddr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		logger.Infof("Connecting to peer: %s", peerAddr)
		err = host.Connect(ctx, *peerInfo)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		host.Peerstore().AddAddr(peerInfo.ID, ma, peerstore.PermanentAddrTTL)
		protocols, err := host.Peerstore().GetProtocols(peerInfo.ID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		logger.Infof("Connected to peer: %s, protocols: %v", peerAddr, protocols)
		boostrappers = append(boostrappers, *peerInfo)
	}

	kdht, err := dht.New(ctx, host,
		dht.Mode(dht.ModeServer),
		dht.V1ProtocolOverride(protocol.ID(protocolID)),
		dht.BootstrapPeers(boostrappers...),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = kdht.Bootstrap(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	time.Sleep(30 * time.Second)
	return kdht, nil
}

func InitHost() (host.Host, error) {
	return libp2p.New(
		libp2p.Identity(nil),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.Security(tls.ID, tls.New),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamuxID, yamuxTransport()),
		libp2p.DefaultTransports,
	)
}

func yamuxTransport() network.Multiplexer {
	tpt := *yamux.DefaultTransport
	tpt.AcceptBacklog = 512
	return &tpt
}

func main() {
	log.SetAllLoggers(log.LevelInfo)
	if err := start(); err != nil {
		logger.Fatalw("failed to start: %+v", err)
	}
}
