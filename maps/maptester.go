package maps

type MapTester struct {
	HzCluster			string
	HzMemberAddresses	[]string
}

type pokedex struct {
	Pokemon	[]pokemon	`json:"pokemon"`
}

type pokemon struct {
	ID				int					`json:"id"`
	Num				string				`json:"num"`
	Name			string				`json:"name"`
	Img				string				`json:"img"`
	ElementType		[]string			`json:"type"`
	Height			string				`json:"height"`
	Weight			string				`json:"weight"`
	Candy			string				`json:"candy"`
	CandyCount		int					`json:"candy_count"`
	EggDistance		string				`json:"egg"`
	SpawnChance		float32				`json:"spawn_chance"`
	AvgSpawns		float32					`json:"avg_spawns"`
	SpawnTime		string				`json:"spawn_time"`
	Multipliers		[]float32			`json:"multipliers"`
	Weaknesses		[]string			`json:"weaknesses"`
	NextEvolution	[]nextEvolution		`json:"next_evolution"`
}

type nextEvolution struct {
	Num		string		`json:"num"`
	Name	string		`json:"name"`
}

func (tester *MapTester) TestMaps() {

	for _, runner := range MapRunners {
		runner.Run("hazeltest-maptester", []string{"10.211.55.6"})
	}

}

