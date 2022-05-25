package shardkv

import "6.824/shardctrler"

func DBdeepcopy(db [shardctrler.NShards]ShardComponent) [shardctrler.NShards]ShardComponent {
	var newdb [shardctrler.NShards]ShardComponent
	for i := 0; i < shardctrler.NShards; i++ {
		newdb[i] = ShardComponent{
			ShardIndex:  db[i].ShardIndex,
			KVDBofShard: mapdeepcopy(db[i].KVDBofShard),
			ClientSeq:   seqmapdeepcopy(db[i].ClientSeq),
			State:       db[i].State,
		}
	}
	return newdb
}

func mapdeepcopy(db map[string]string) map[string]string {
	newdb := make(map[string]string)
	for k, v := range db {
		newdb[k] = v
	}
	return newdb
}

func seqmapdeepcopy(db map[int64]int64) map[int64]int64 {
	newdb := make(map[int64]int64)
	for k, v := range db {
		newdb[k] = v
	}
	return newdb
}
