package shardkv

func (kv *ShardKV) checkconfig() {
	newcfg := kv.mck.Query(kv.config.Num + 1)
	kv.mu.Lock()
	if newcfg.Num <= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	DPrintf("[%d,%d,%d]: checknewconfig: %v", kv.gid, kv.me, kv.config.Num, newcfg)
	kv.mu.Unlock()
	kv.rf.Start(ConfigOp{
		Config: newcfg,
	})
}

func (kv *ShardKV) ApplyConfigOp(op ConfigOp, raftindex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[%d,%d,%d]: ApplyConfigOp: %v", kv.gid, kv.me, kv.config.Num, op)
	if op.Config.Num <= kv.config.Num {
		return
	}
	oldcfg := kv.config
	kv.config = op.Config
	kv.checkShadeMigrate(oldcfg)
}
