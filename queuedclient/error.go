package queuedclient

import (
	"errors"
	"strings"
)

var (
	// ErrNoServers - настройки серверов не прописаны в конфиге
	ErrNoServers = errors.New("no servers defined in onlineconf")

	// ErrEmptyResponse - queued ответил пустым пакетом
	ErrEmptyResponse = errors.New("empty response from queued")

	// ErrInvalidShard - номер шарда должен быть меньше кол-ва шардов (QueuedClient.ShardNum)
	ErrInvalidShard = errors.New("invalid shard number")

	// ErrPanic - при выполнении мультишардовой операции произошла паника
	ErrPanic = errors.New("caught panic during multishard request")
)

// ErrMulti - набор ошибок от нескольких шардов.
//
// В некоторых методах, ошибка одного шарда при успехе на остальных не должны приводить к отмене
// операции - полученные данные нужно использовать, а о произошедших ошибках просто записать в лог.
//
// TODO зарефакторить, чтобы не поддерживало интерфейс error, чтобы не было соблазна вернуть как есть
// (и долго ловить, почему это err != nil, когда ошибок не было). Вынести в utils
type ErrMulti []error

var _ error = ErrMulti{}

// Error возвращает список ошибок через запятую.
func (errs ErrMulti) Error() string {
	strs := make([]string, len(errs))

	for i, err := range errs {
		strs[i] = err.Error()
	}

	return strings.Join(strs, ", ")
}

func (errs *ErrMulti) Append(err error) {
	*errs = append(*errs, err)
}

func (errs ErrMulti) ToError() error {
	if len(errs) == 0 {
		return nil
	}

	return errs
}
