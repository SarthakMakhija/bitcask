package key

type Serializable interface {
	Serialize() []byte
}
