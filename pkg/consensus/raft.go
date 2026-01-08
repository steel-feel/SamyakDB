package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/steel-feel/SamyakDB/pkg/storage"
)

type FSM struct {
	store *storage.Storage
}

type LogEntry struct {
	Op    string
	Key   string
	Value []byte
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var entry LogEntry
	if err := json.Unmarshal(l.Data, &entry); err != nil {
		log.Printf("failed to unmarshal log entry: %v", err)
		return err
	}

	switch entry.Op {
	case "put":
		f.store.Put(entry.Key, entry.Value)
		return nil
	case "delete":
		f.store.Delete(entry.Key)
		return nil
	default:
		return fmt.Errorf("unknown operation: %s", entry.Op)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &Snapshot{store: f.store}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	decoder := json.NewDecoder(rc)
	for decoder.More() {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			return err
		}
		f.store.Put(entry.Key, entry.Value)
	}
	return nil
}

type Snapshot struct {
	store *storage.Storage
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		encoder := json.NewEncoder(sink)
		var err error
		s.store.Iterate(func(key string, value []byte) bool {
			err = encoder.Encode(LogEntry{
				Op:    "put",
				Key:   key,
				Value: value,
			})
			return err == nil
		})
		return err
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}

type RaftNode struct {
	Raft      *raft.Raft
	store     *storage.Storage
	raftDir   string
	raftAddr  string
	nodeID    string
}

func NewRaftNode(nodeID, raftDir, raftAddr string, store *storage.Storage, bootstrap bool) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	// Relaxing constraints for testing/M4
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 1024

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, err
	}
	
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	fsm := &FSM{store: store}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		log.Printf("Bootstrapping cluster as %s", nodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			log.Printf("Bootstrap error (ignored if already bootstrapped): %v", err)
		}
	}

	return &RaftNode{
		Raft:     r,
		store:    store,
		raftDir:  raftDir,
		raftAddr: raftAddr,
		nodeID:   nodeID,
	}, nil
}

func (rn *RaftNode) Apply(op, key string, value []byte) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	entry := LogEntry{Op: op, Key: key, Value: value}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	f := rn.Raft.Apply(data, 5*time.Second)
	return f.Error()
}

func (rn *RaftNode) Join(nodeID, addr string) error {
	log.Printf("Received join request for remote node %s at %s", nodeID, addr)

	configFuture := rn.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("Failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := rn.Raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := rn.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	log.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}
