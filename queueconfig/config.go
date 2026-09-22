package queueconfig

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onlineconf/onlineconf-go/v2"
	"kythe.io/kythe/go/util/datasize"
)

const globalPath = "/tp/go"

// CSVRegex - значения в конфиге через запятую, с опциональными пробелами.
var CSVRegex = regexp.MustCompile(`\s*,\s*`)

// OnlineConf - зависимость от онлайнконфа: часть интерфейса onlineconf-go/v2 (Module/Subtree),
// которая нужна tp.
type OnlineConf interface {
	GetString(string, string) string
	GetStringIfExists(string) (string, bool)
	GetInt(string, int) int
	GetIntErr(string) (int, error)
	Subscribe(string) (chan struct{}, error)
	// SubscribeChan/UnsubscribeChan - подписка на изменение значения по пути через переданный
	// канал (семантика onlineconf-go/v2: уведомление - неблокирующая отправка struct{}{},
	// UnsubscribeChan закрывает канал). Нужны для горячего переподключения к queued при
	// смене списка серверов (см. queuedclient.Dial).
	SubscribeChan(string, chan<- struct{}) error
	UnsubscribeChan(string, chan<- struct{})
}

// OnlineConfWithSubtree - зависимость от онлайнконфа с методом Subtree.
// Соответствует как интерфейсу библиотеки onlineconf-go/v2, так и mock-интерфейсу,
// который используется для тестов.
type OnlineConfWithSubtree[T OnlineConf] interface {
	OnlineConf
	Subtree(string) T
}

// Config - настройки обработчика очередей. Можно пользоваться, как обычным объектом с интерфейсом OnlineConf для получения настроек относительно корня проекта.
// Также предусмотрены методы для получения настроек per-instance и pre-queue.
type Config struct {
	OnlineConf            // для работы с глобальными настройками уровня проекта - например, списком серверов очередей - можно пользоваться этим объектом, как обычным интерфейсом OnlineConf
	global     OnlineConf // глобальные настройки очередей (/tp/go)
	instance   OnlineConf // настройки инстанса (/tp/go/$InstanceName)
	pathErrors sync.Map   // сюда пишутся пути, про которые уже жаловались в лог. обращение к мэпу идёт только при ошибках.
}

// New возвращает новый объект конфига.
func New[T OnlineConf](conf OnlineConfWithSubtree[T], instanceName string) *Config {
	return &Config{
		OnlineConf: conf,
		global:     conf.Subtree(globalPath),
		instance:   conf.Subtree(path.Join(globalPath, instanceName)),
	}
}

// Subscribe подписывается на изменения внутри поддерева /tp/go
func (c *Config) Subscribe() (chan struct{}, error) {
	return c.global.Subscribe("/")
}

func (c *Config) getStringPerInstance(confPath string) (string, error) {
	if s, ok := c.instance.GetStringIfExists(confPath); ok {
		return s, nil
	}

	if s, ok := c.global.GetStringIfExists(confPath); ok {
		return s, nil
	}

	return "", errNotFound(confPath)
}

// в случае, если не найдено ничего, в ошибке возвращается путь про per-queue значение
// NB в методах get*PerQueue используется [path.Join], пэтому указывать ведущий слеш в confPath
// не требуется, однако это сделано для соответствия стиля с PerInstance
func (c *Config) getStringPerQueue(queueID uint16, confPath string) (str, usedPath string, err error) {
	perQueuePath := path.Join("/", strconv.Itoa(int(queueID)), confPath)
	if s, ok := c.instance.GetStringIfExists(perQueuePath); ok {
		return s, perQueuePath, nil
	}

	s, err := c.getStringPerInstance(confPath)
	/*
		if err == nil {
			return s, confPath, nil
		}

		if err.isNotFound {
			err.Path = perQueuePath
		}

		return "", perQueuePath, err
	*/
	return s, confPath, err
}

func (c *Config) getIntPerInstance(confPath string) (int, error) {
	i, err := c.instance.GetIntErr(confPath)
	if err == nil {
		return i, nil
	}

	if !errors.Is(err, onlineconf.ErrNotFound) {
		return 0, err
	}

	i, err = c.global.GetIntErr(confPath)
	if err == nil {
		return i, nil
	}

	if !errors.Is(err, onlineconf.ErrNotFound) {
		return 0, err
	}

	return 0, errNotFound(confPath)
}

// в случае, если не найдено ничего, в ошибке возвращается путь про per-queue значение
// NB в методах get*PerQueue используется [path.Join], пэтому указывать ведущий слеш в confPath
// не требуется, однако это сделано для соответствия стиля с PerInstance
func (c *Config) getIntPerQueue(queueID uint16, confPath string) (int, error) {
	perQueuePath := path.Join("/", strconv.Itoa(int(queueID)), confPath)

	i, err := c.instance.GetIntErr(perQueuePath)
	if err == nil {
		return i, nil
	}

	if !errors.Is(err, onlineconf.ErrNotFound) {
		return 0, err
	}

	i, err = c.getIntPerInstance(confPath)
	if err == nil {
		return i, nil
	}

	if !isNotFound(err) {
		return 0, err
	}

	return 0, errNotFound(perQueuePath)
}

func (c *Config) getSizePerInstance(confPath string) (uint64, error) {
	s, err := c.getStringPerInstance(confPath)
	if err != nil {
		return 0, err
	}

	size, err := datasize.Parse(s)
	if err != nil {
		return 0, &ConfError{
			Path:  confPath,
			Value: s,
			Err:   err,
		}
	}

	return uint64(size), nil
}

// parseDuration разбирает длительность: голое число (в т.ч. дробное) трактуется как секунды -
// для совместимости с перловым конфигом. Иначе - синтаксис [time.ParseDuration].
// Алгоритм идентичен парзингу onlineconf-go v2 (там он не экспортирован).
func parseDuration(s string) (time.Duration, error) {
	if strings.ContainsAny(s, "hms") { // доли секунды тоже содержат s
		return time.ParseDuration(s)
	}

	return time.ParseDuration(s + "s")
}

// GetDuration получает длительность по указанному глобальному (не per-instance) пути.
// Если путь не найден - возвращается дефолт. В случае ошибки парзинга - в лог пишется об этом, и возвращается дефолт.
func (c *Config) GetDuration(ctx context.Context, confPath string, dfl time.Duration) time.Duration {
	s, ok := c.GetStringIfExists(confPath)
	if !ok {
		return dfl
	}

	d, err := parseDuration(s)
	if err == nil {
		return d
	}

	err = &ConfError{
		Path:    confPath,
		Value:   s,
		Err:     err,
		Default: dfl,
	}

	c.logError(ctx, err, nil, "using the default")

	return dfl
}

func (c *Config) getDurationPerInstance(confPath string) (time.Duration, error) {
	s, err := c.getStringPerInstance(confPath)
	if err != nil {
		return 0, err
	}

	d, err := parseDuration(s)
	if err != nil {
		return 0, &ConfError{
			Path:  confPath,
			Value: s,
			Err:   err,
		}
	}

	return d, nil
}

func (c *Config) getDurationPerQueue(queueID uint16, confPath string) (time.Duration, error) {
	s, usedPath, err := c.getStringPerQueue(queueID, confPath)
	if err != nil {
		return 0, err
	}

	d, err := parseDuration(s)
	if err != nil {
		return 0, &ConfError{
			Path:  usedPath,
			Value: s,
			Err:   err,
		}
	}

	return d, nil
}

func (c *Config) getFloatPerInstance(confPath string) (float64, error) {
	s, err := c.getStringPerInstance(confPath)
	if err != nil {
		return 0, err
	}

	ret, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, &ConfError{
			Path:  confPath,
			Value: s,
			Err:   err,
		}
	}

	return ret, nil
}

func (c *Config) getFloatPerQueue(queueID uint16, confPath string) (float64, error) {
	s, usedPath, err := c.getStringPerQueue(queueID, confPath)
	if err != nil {
		return 0, err
	}

	ret, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, &ConfError{
			Path:  usedPath,
			Value: s,
			Err:   err,
		}
	}

	return ret, nil
}
