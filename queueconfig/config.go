package queueconfig

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/onlineconf/onlineconf-go/v2"
	"kythe.io/kythe/go/util/datasize"

	"gitlab.cebc.tools/go/utils.git/mytime"
)

const globalPath = "/tp/go"

// CSVRegex - значения в конфиге через запятую, с опциональными пробелами.
var CSVRegex = regexp.MustCompile(`\s*,\s*`)

// OnlineConfImpl - зависимость от библиотеки onlineconf-go/v2.
// Интерфейс не совместим с используемым внутри [OnlineConf], если необходим этот интерфейс,
// нужно использовать [NewAdapter]
type OnlineConfImpl interface {
	Subtree(string) *onlineconf.Subtree
	GetString(string, string) string
	GetStringIfExists(string) (string, bool)
	GetInt(string, int) int
	GetIntErr(string) (int, error)
	Subscribe(path string) (chan struct{}, error)
}

// OnlineConfAdapter обеспечивает совместимость библиотеки onlineconf-go/v2 с используемым
// внутри tp представлением [OnlineConf].
type OnlineConfAdapter struct {
	OnlineConfImpl
}

// NewAdapter преобразует интерфейс, реализуемый библиотекой onlineconf-go/v2 во внутреннее
// представление tp ([OnlineConf]).
func NewAdapter(conf OnlineConfImpl) OnlineConf {
	return OnlineConfAdapter{OnlineConfImpl: conf}
}

func (oconf OnlineConfAdapter) Subtree(path string) OnlineConf {
	return OnlineConfAdapter{OnlineConfImpl: oconf.OnlineConfImpl.Subtree(path)}
}

// OnlineConf - зависимость от абстрактного онлайнконфа (без зависимости от внешней библиотеки onlineconf-go/v2).
// Используется внутри tp.
type OnlineConf interface {
	Subtree(string) OnlineConf
	GetString(string, string) string
	GetStringIfExists(string) (string, bool)
	GetInt(string, int) int
	GetIntErr(string) (int, error)
	Subscribe(path string) (chan struct{}, error)
}

// Config - настройки обработчика очередей. Можно пользоваться, как обычным объектом с интерфейсом Onlineconf для получения настроек относительно корня проекта.
// Также предусмотрены методы для получения настроек per-instance и pre-queue.
type Config struct {
	OnlineConf            // для работы с глобальными настройками уровня проекта - например, списком серверов очередей - можно пользоваться этим объектом, как обычным интерфейсом OnlineConf
	global     OnlineConf // глобальные настройки очередей (/tp/go)
	instance   OnlineConf // настройки инстанса (/tp/go/$InstanceName)
	pathErrors sync.Map   // сюда пишутся пути, про которые уже жаловались в лог. обращение к мэпу идёт только при ошибках.
}

// New возвращает новый объект конфига. oconf - корень проекта (не обязательно TREE).
func New(conf OnlineConfImpl, instanceName string) *Config {
	return NewInternal(OnlineConfAdapter{OnlineConfImpl: conf}, instanceName)
}

// NewInternal возвращает новый объект конфига. oconf - корень проекта (не обязательно TREE).
// Для внутреннего использования, а также для тестов.
func NewInternal(conf OnlineConf, instanceName string) *Config {
	return &Config{
		OnlineConf: conf,
		global:     conf.Subtree(globalPath),
		instance:   conf.Subtree(path.Join(globalPath, instanceName)),
	}
}

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

// GetDuration получает длительность по указанному глобальному (не per-instance) пути.
// Если путь не найден - возвращается дефолт. В случае ошибки парзинга - в лог пишется об этом, и возвращается дефолт.
func (c *Config) GetDuration(ctx context.Context, confPath string, dfl time.Duration) time.Duration {
	s, ok := c.GetStringIfExists(confPath)
	if !ok {
		return dfl
	}

	d, err := mytime.ParseDuration(s)
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

	d, err := mytime.ParseDuration(s)
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

	d, err := mytime.ParseDuration(s)
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
