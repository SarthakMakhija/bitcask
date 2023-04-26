package key

type Serializable interface {
	Serialize() []byte
}

type BitCaskKey interface {
	comparable
	Serializable
}
