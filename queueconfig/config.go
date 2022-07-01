package queueconfig

import (
	"context"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"kythe.io/kythe/go/util/datasize"

	"gitlab.corp.mail.ru/mygo/utils.git/mytime"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

const globalPath = "/tp/go"

// CSVRegex - значения в конфиге через запятую, с опциональными пробелами.
var CSVRegex = regexp.MustCompile(`\s*,\s*`)

// TODO чтобы не писать доку (а без доки всё равно юзеру это имя покажут) таким внутренним вещам, которые нужны в неск.
// пакетах, их надо перенести в internal/. Как их поднакопится - надо сделать.

// Config - настройки обработчика очередей. Можно пользоваться, как обычным объектом с интерфейсом Onlineconf для получения настроек относительно корня проекта.
// Также предусмотрены методы для получения настроек per-instance и pre-queue.
type Config struct {
	oconf.OnlineConf                  // для работы с глобальными настройками уровня проекта - например, списком серверов очередей - можно пользоваться этим объектом, как обычным интерфейсом OnlineConf
	global           oconf.OnlineConf // глобальные настройки очередей (/tp/go)
	instance         oconf.OnlineConf // настройки инстанса (/tp/go/$InstanceName)
	pathErrors       sync.Map         // сюда пишутся пути, про которые уже жаловались в лог. обращение к мэпу идёт только при ошибках.
}

// New возвращает новый объект конфига. oconf - корень проекта (не обязательно TREE).
func New(conf oconf.OnlineConf, instanceName string) *Config {
	return &Config{
		OnlineConf: conf,
		global:     oconf.NewSubtree(conf, globalPath), // TODO отрефакторить наконец онлайнконф на гитхабе!
		instance:   oconf.NewSubtree(conf, path.Join(globalPath, instanceName)),
	}
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
func (c *Config) getStringPerQueue(queueID uint16, confPath string) (str, usedPath string, err error) {
	perQueuePath := path.Join(strconv.Itoa(int(queueID)), confPath)
	if s, ok := c.instance.GetStringIfExists(perQueuePath); ok {
		return s, perQueuePath, nil
	}

	s, err := c.getStringPerInstance(confPath)
	/* см. ниже коммент в getIntPerQueue
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
	if i, ok := c.instance.GetIntIfExists(confPath); ok {
		return i, nil
	}

	if i, ok := c.global.GetIntIfExists(confPath); ok {
		return i, nil
	}

	return 0, errNotFound(confPath)
}

// в случае, если не найдено ничего, в ошибке возвращается путь про per-queue значение
func (c *Config) getIntPerQueue(queueID uint16, confPath string) (int, error) {
	perQueuePath := path.Join(strconv.Itoa(int(queueID)), confPath)
	if i, ok := c.instance.GetIntIfExists(perQueuePath); ok {
		return i, nil
	}

	i, err := c.getIntPerInstance(confPath)
	/* раскомментить, если вдруг понадобится инфа об очереди, для которой не нашли параметра. обычно это нужно
	   при ошибках парзинга - а про не найденный параметр вроде как полезнее будет показать, что отсутствует дефолт.

		if err == nil {
			return i, nil
		}

		if err.isNotFound {
			err.Path = perQueuePath
		}

		return 0, err
	*/
	return i, err
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
