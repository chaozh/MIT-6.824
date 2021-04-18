package models

import "6.824/porcupine"
import "fmt"
import "sort"

type KvInput struct {
	Op    uint8 // 0 => get, 1 => put, 2 => append
	Key   string
	Value string
}

type KvOutput struct {
	Value string
}

var KvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(KvInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	Init: func() interface{} {
		// note: we are modeling a single key's value here;
		// we're partitioning by key, so this is okay
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(KvInput)
		out := output.(KvOutput)
		st := state.(string)
		if inp.Op == 0 {
			// get
			return out.Value == st, state
		} else if inp.Op == 1 {
			// put
			return true, inp.Value
		} else {
			// append
			return true, (st + inp.Value)
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(KvInput)
		out := output.(KvOutput)
		switch inp.Op {
		case 0:
			return fmt.Sprintf("get('%s') -> '%s'", inp.Key, out.Value)
		case 1:
			return fmt.Sprintf("put('%s', '%s')", inp.Key, inp.Value)
		case 2:
			return fmt.Sprintf("append('%s', '%s')", inp.Key, inp.Value)
		default:
			return "<invalid>"
		}
	},
}
