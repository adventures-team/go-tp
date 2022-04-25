package queueconfig

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"kythe.io/kythe/go/util/datasize"

	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

const globalPath = "/tp/go"

var (
	ErrNotFound = errors.New("path not found")
	CSVRegex    = regexp.MustCompile(`\s*,\s*`)
)

// Config - настройки обработчика очередей. Можно пользоваться, как обычным объектом с интерфейсом Onlineconf для получения настроек относительно корня проекта.
// Также предусмотрены методы для получения настроек per-instance и pre-queue.
type Config struct {
	oconf.OnlineConf                  // для работы с глобальными настройками уровня проекта - например, списком серверов очередей - можно пользоваться этим объектом, как обычным интерфейсом OnlineConf
	global           oconf.OnlineConf // глобальные настройки очередей (/tp/go)
	instance         oconf.OnlineConf // настройки инстанса (/tp/go/$InstanceName)
}

func New(conf oconf.OnlineConf, instanceName string) *Config {
	return &Config{
		OnlineConf: conf,
		global:     oconf.NewSubtree(conf, globalPath), // TODO отрефакторить наконец онлайнконф на гитхабе!
		instance:   oconf.NewSubtree(conf, path.Join(globalPath, instanceName)),
	}
}

func (c *Config) getStringPerInstance(confPath string) (string, bool) {
	if s, ok := c.instance.GetStringIfExists(confPath); ok {
		return s, true
	}

	return c.global.GetStringIfExists(confPath)
}

func (c *Config) getStringPerQueue(queueID uint16, confPath string) (string, bool) {
	if s, ok := c.instance.GetStringIfExists(path.Join(strconv.Itoa(int(queueID)), confPath)); ok {
		return s, true
	}

	return c.getStringPerInstance(confPath)
}

func (c *Config) getIntPerInstance(confPath string) (int, bool) {
	if i, ok := c.instance.GetIntIfExists(confPath); ok {
		return i, true
	}

	return c.global.GetIntIfExists(confPath)
}

func (c *Config) getIntPerQueue(queueID uint16, confPath string) (int, bool) {
	if i, ok := c.instance.GetIntIfExists(path.Join(strconv.Itoa(int(queueID)), confPath)); ok {
		return i, true
	}

	return c.getIntPerInstance(confPath)
}

func (c *Config) getSizePerInstance(confPath string) (uint64, error) {
	s, ok := c.getStringPerInstance(confPath)
	if !ok {
		return 0, fmt.Errorf("%s: %w", confPath, ErrNotFound)
	}

	size, err := datasize.Parse(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", confPath, s, err)
	}

	return uint64(size), nil
}

// ParseDuration парзит длительность при помощи time.ParseDuration. Если единицы измерения не указаны - считается, что указаны секунды.
func ParseDuration(s string) (time.Duration, error) {
	if strings.ContainsAny(s, "hms") { // доли секунды тоже содержат s
		return time.ParseDuration(s)
	}

	return time.ParseDuration(s + "s")
}

// GetDuration получает длительность по указанному глобальному (не per-instance) пути.
// Если путь не найден и указан дефолт - возвращается он, иначе - ошибка ErrNotFound.
//
// В случае других ошибок, если задан дефолт - возвращается и дефолт, и ошибка, для упрощения
// реализации кейса, когда дефолт нужен всегда, а ошибка просто пишется в лог.
func (c *Config) GetDuration(confPath string, dfl ...time.Duration) (time.Duration, error) {
	s, ok := c.GetStringIfExists(confPath)
	if !ok {
		if len(dfl) != 0 {
			return dfl[0], nil
		}

		return time.Duration(0), fmt.Errorf("%s: %w", confPath, ErrNotFound)
	}

	d, err := ParseDuration(s)
	if err != nil {
		if len(dfl) != 0 {
			d = dfl[0]
			err = fmt.Errorf("%s: %s: %w, using default %v", confPath, s, err, d)
		} else {
			err = fmt.Errorf("%s: %s: %w", confPath, s, err)
		}
	}

	return d, err
}

func (c *Config) getDurationPerInstance(confPath string) (time.Duration, error) {
	s, ok := c.getStringPerInstance(confPath)
	if !ok {
		return 0, fmt.Errorf("%s: %w", confPath, ErrNotFound)
	}

	d, err := ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", confPath, s, err)
	}

	return d, nil
}

func (c *Config) getDurationPerQueue(queueID uint16, confPath string) (time.Duration, error) {
	s, ok := c.getStringPerQueue(queueID, confPath)
	if !ok {
		return 0, fmt.Errorf("%d/%s: %w", queueID, confPath, ErrNotFound)
	}

	d, err := ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%d/%s: %s: %w", queueID, confPath, s, err)
	}

	return d, nil
}

func (c *Config) getFloatPerInstance(confPath string) (float64, error) {
	s, ok := c.getStringPerInstance(confPath)
	if !ok {
		return 0, fmt.Errorf("%s: %w", confPath, ErrNotFound)
	}

	ret, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", confPath, s, err)
	}

	return ret, nil
}
