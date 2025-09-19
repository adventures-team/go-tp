package internal

// AllocSize - capacity буфера кодирования отправляемых пакетов
const AllocSize = 64

// AllocBytes выделяет буфер с capacity AllocSize
func AllocBytes() []byte {
	return make([]byte, 0, AllocSize)
}
