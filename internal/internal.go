package internal

// AllocSize - capacity буфера кодирования отправляемых пакетов
const AllocSize = 64

func AllocBytes() []byte {
	return make([]byte, 0, AllocSize)
}
