package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) ApplySnapshotMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		return
	}
	DPrintf("[%d,%d,%d]: ApplySnapshotMsg: InstallSnapshot", kv.gid, kv.me, kv.config.Num)
	kv.ReadSnapShot(msg.Snapshot)
	kv.RaftlastIncludedIndex = msg.SnapshotIndex
}

func (kv *ShardKV) ReadSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("[%d,%d,%d]: ReadSnapShot: no snapshot", kv.gid, kv.me)
		return
	}
	var tmpkvDB [shardctrler.NShards]ShardComponent
	var tmpConfig shardctrler.Config
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&tmpkvDB) != nil ||
		d.Decode(&tmpConfig) != nil {
		DPrintf("[%d,%d,%d]: ReadSnapShot: decode error", kv.gid, kv.me)
		return
	}
	kv.kvDB = tmpkvDB
	kv.config = tmpConfig
	DPrintf("[%d,%d,%d]: ReadSnapShot: %v", kv.gid, kv.me, kv.config.Num, kv.kvDB)
}

func (kv *ShardKV) TryMakeSnapshot(raftIndex int, force bool) {
	kv.mu.Lock()
	if kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	if !force && kv.rf.GetRaftStateSize() < kv.maxraftstate/10*proportion {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(DBdeepcopy(kv.kvDB))
	e.Encode(kv.config)
	data := w.Bytes()
	DPrintf("[%d,%d,%d]: TryMakeSnapshot: %d,%v", kv.gid, kv.me, kv.config.Num, raftIndex, kv.kvDB)
	kv.mu.Unlock()
	kv.rf.Snapshot(raftIndex, data)
	kv.mu.Lock()
}
