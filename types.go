package gohelix

type HelixConfigScope string

type AddResourceOption struct {
	Partitions               int
	StateModel               string
	RebalancerMode           string
	RebalanceStrategy        string
	BucketSize               int
	MaxPartitionsPerInstance int
}

func DefaultAddResourceOption(partitions int, stateModel string) AddResourceOption {
	return AddResourceOption{
		Partitions:     partitions,
		StateModel:     stateModel,
		RebalancerMode: "SEMI_AUTO",
	}
}

func (opt AddResourceOption) validate() error {
	if opt.Partitions < 1 || opt.StateModel == "" || opt.RebalancerMode == "" {
		return ErrInvalidAddResourceOption
	}
	return nil
}
