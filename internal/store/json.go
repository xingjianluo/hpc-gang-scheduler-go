package store

import "encoding/json"

// 快照落盘的外层信封。
type fileEnvelope struct {
	Version int64  `json:"version"`
	Data    []byte `json:"data"`
}

func jsonMarshal(snap Snapshot) ([]byte, error) {
	return json.Marshal(fileEnvelope{Version: snap.Version, Data: snap.Data})
}

func jsonUnmarshal(data []byte, snap *Snapshot) error {
	var env fileEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	snap.Version = env.Version
	snap.Data = env.Data
	return nil
}
