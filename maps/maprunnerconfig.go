package maps

type MapConfig struct {
	NumMaps                   int
	NumRuns                   int
	MapBaseName               string
	UseMapPrefix              bool
	MapPrefix                 string
	AppendMapIndexToMapName   bool
	AppendClientIdToMapName   bool
	SleepBetweenActionBatches *SleepConfig
	SleepBetweenRuns          *SleepConfig
}

type SleepConfig struct {
	Enabled    bool
	DurationMs int
}
