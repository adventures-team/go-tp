/*
queued-mover переносит события очередей из одного кластера queued (источник, A) в другой
(приёмник, B): читает активные события из шардов A, публикует их в B как есть (тело события
не декодируется) и удаляет из A. Логика - пакет internal/mover, краткое описание - в
docs/README.md.

# Запуск

	queued-mover -subtree /project/queued-mover [-module <name|path>]

Ключи командной строки:

  - -subtree - обязательный: поддерево onlineconf с настройками утилиты; без него - usage и
    код выхода 2.
  - -module - модуль onlineconf: имя в /usr/local/etc/onlineconf или путь к cdb-файлу
    (значение со слешем). По умолчанию - переменная окружения CDB_CONFIG_FILE, если она
    не задана или пуста - TREE.

Остальные настройки - только в onlineconf, большинство перечитывается на лету.

# Конфигурация

Кластеры задаются в поддеревьях src и dst в формате ключа queueds этого проекта: список
шардов через запятую в queueds/multi и необязательные параметры соединения рядом
(queueds/multi/timeout и т.д., см. константы queuedclient.Conf*):

	$Root/src/queueds/multi   = queued-old-1:20000,queued-old-2:20000
	$Root/dst/queueds/multi   = queued-new-1:20000,queued-new-2:20000
	$Root/queue_ids           = 1122,1123   # очереди для переноса, формат /tp/go/queue_ids (обязателен)
	$Root/one_shot            = 0           # 1 - выйти, когда во всех очередях A не осталось событий
	$Root/preserve_ids        = 0           # 1 - сохранять ID событий (см. ниже)
	$Root/workers             = 1           # горутин на очередь
	$Root/fetch_batch_size    = 100         # событий за один запрос с каждого шарда A
	$Root/initial_delay       = 0           # задержка активации перенесённого события в B
	$Root/defer_time          = 1m          # на сколько откладывать в A событие, не попавшее в B
	$Root/poll_delay          = 1s          # пауза после пустой выборки и после пачки с ошибками
	$Root/stat_period         = 1m          # период строки прогресса и перечитывания queue_ids/workers
	$Root/log_level           = info

Длительности: голое число - секунды, иначе синтаксис time.ParseDuration.

# Режимы

По умолчанию утилита работает как сервис: переносит события по мере их появления и
активации в A, пока не получит SIGTERM/SIGINT (текущие итерации доводятся до конца:
каждое выбранное событие удаляется из A или откладывается в A). SIGHUP перечитывает
queue_ids, workers и log_level; изменения остальных параметров действуют и так.

С one_shot = 1 утилита выходит, когда во всех очередях из queue_ids по статистике A не
осталось событий - ни активных, ни отложенных, ни заблокированных. Отложенные события
переносятся по мере их активации (queued отдаёт клиенту только активные события), поэтому
утилита ждёт их; события, заблокированные другими клиентами или оставшиеся
заблокированными после аварийного завершения, переносятся после снятия блокировки по
таймауту queued. Продюсеры должны быть переключены на B до запуска: проверка - снимок.

Гарантия - at-least-once: сначала публикация в B, потом удаление из A, падение между ними
даёт повтор переноса после переактивации события в A (дубль в B с новыми ID, идемпотентный
повтор с сохранением ID).

# ID событий

По умолчанию события создаются в B с новыми ID (их назначает queued B, шард - по round
robin), коллизии невозможны.

preserve_ids = 1 - переносить события с исходными ID: событие кладётся в шард B по тому
же правилу, что tp.Queue.Publish для событий с явным ID (queuedclient.CalcShard). Если в
B под этим ID уже есть событие с тем же телом - это дубль (например, предыдущий запуск
упал между публикацией и удалением), он считается перенесённым; с другим телом - коллизия
ID: событие остаётся в A и повторяется до вмешательства оператора.

Внимание: сохранение ID может привести к коллизиям ID в кластере B - как при переносе,
так и позже. queued назначает ID автоинкрементом отдельно в каждом шарде, поэтому:
(1) события из разных шардов A могут иметь одинаковые ID - при переносе они попадают в
один шард B, второе из них не переносится и остаётся в A с ошибкой collision в логе;
(2) ID, выданные queued B после переноса (или уже выданные раньше), могут совпасть с
перенесёнными: публикация такого события в B завершится ошибкой "wrong item", а с
IgnoreDuplicateItems (FlagIgnoreWrongItem) будет молча проигнорирована - событие
продюсера пропадёт. Включайте только когда ID событий генерирует приложение
(tp.EventIDBuilder), они уникальны и обязаны сохраниться - например, в ID упакованы
данные (tp.Unpacker) или по ID обеспечивается идемпотентность.

# Коды выхода

	0 - штатное завершение: сигнал в режиме сервиса, все очереди пусты в one-shot
	1 - ошибка: конфиг, модуль onlineconf, соединение с queued
	2 - ошибка командной строки
	3 - one-shot прерван сигналом до опустошения очередей
*/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/onlineconf/onlineconf-go/v2"

	"github.com/adventures-team/go-tp"
	"github.com/adventures-team/go-tp/internal/mover"
	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
)

const (
	// EnvModule - переменная окружения с модулем onlineconf: значение -module по умолчанию.
	EnvModule = "CDB_CONFIG_FILE"

	// DefaultModule - модуль onlineconf, если не заданы ни -module, ни EnvModule.
	DefaultModule = "TREE"

	// instanceName - имя инстанса для queueconfig клиентов (поддерево tp/go внутри src/dst не используется).
	instanceName = "queued-mover"

	// Коды выхода.
	exitOK          = 0
	exitError       = 1
	exitUsage       = 2
	exitInterrupted = 3
)

// Ошибки командной строки.
var (
	errSubtreeRequired = errors.New("-subtree is mandatory")
	errSubtreeRelative = errors.New("-subtree must start with /")
	errModuleEmpty     = errors.New("-module must not be empty")
	errExtraArgs       = errors.New("unexpected arguments")
)

// options - параметры командной строки.
type options struct {
	module  string
	subtree string
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv(EnvModule), os.Stderr))
}

// run выполняет утилиту и возвращает код выхода. envModule - значение CDB_CONFIG_FILE.
func run(args []string, envModule string, stderr io.Writer) int {
	opt, err := parseOptions(args, envModule, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}

		_, _ = fmt.Fprintln(stderr, "queued-mover:", err)

		return exitUsage
	}

	logger := advslog.Init()

	advslog.Level.Set(mover.DefaultLogLevel) // до применения log_level из конфига

	ctx := advslog.NewContext(context.Background(), logger)

	module, err := onlineconf.OpenModule(opt.module)
	if err != nil {
		logger.Error("cannot open the onlineconf module", "module", opt.module, advslog.Err(err))

		return exitError
	}

	root := module.Subtree(opt.subtree)

	src, err := dial(ctx, root, "src")
	if err != nil {
		logger.Error("cannot create the source queued client", advslog.Err(err))

		return exitError
	}

	dst, err := dial(ctx, root, "dst")
	if err != nil {
		logger.Error("cannot create the destination queued client", advslog.Err(err))

		return exitError
	}

	m := mover.New(root, src, dst, logger)

	ctx, stop := signal.NotifyContext(ctx, tp.SignalsExit[:]...)
	defer stop()

	go reloadOnSignal(ctx, m, logger)

	logger.Info("queued-mover started", "module", opt.module, "subtree", opt.subtree,
		"src_shards", src.ShardNum(), "dst_shards", dst.ShardNum())

	err = m.Run(ctx)

	switch {
	case err == nil:
		logger.Info("queued-mover finished")

		return exitOK

	case errors.Is(err, mover.ErrInterrupted):
		logger.Warn("queued-mover interrupted", advslog.Err(err))

		return exitInterrupted

	default:
		logger.Error("queued-mover failed", advslog.Err(err))

		return exitError
	}
}

// parseOptions разбирает командную строку; usage печатается в stderr.
func parseOptions(args []string, envModule string, stderr io.Writer) (options, error) {
	var opt options

	fs := flag.NewFlagSet("queued-mover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opt.module, "module", moduleDefault(envModule),
		"onlineconf module: a name in /usr/local/etc/onlineconf or a path to a cdb file (default: $"+EnvModule+", else "+DefaultModule+")")
	fs.StringVar(&opt.subtree, "subtree", "", "onlineconf subtree with the tool settings, e.g. /project/queued-mover (mandatory)")

	if err := fs.Parse(args); err != nil {
		return opt, err
	}

	err := validateOptions(&opt, fs.NArg())
	if err != nil {
		fs.Usage()
	}

	return opt, err
}

// validateOptions проверяет и нормализует параметры.
func validateOptions(opt *options, extraArgs int) error {
	switch {
	case extraArgs != 0:
		return errExtraArgs
	case opt.subtree == "":
		return errSubtreeRequired
	case !strings.HasPrefix(opt.subtree, "/"):
		return errSubtreeRelative
	case opt.module == "":
		return errModuleEmpty
	}

	opt.subtree = path.Clean(opt.subtree)

	return nil
}

// moduleDefault возвращает модуль onlineconf по умолчанию: CDB_CONFIG_FILE, иначе TREE.
func moduleDefault(envModule string) string {
	if envModule != "" {
		return envModule
	}

	return DefaultModule
}

// dial создаёт клиент queued по поддереву side (src или dst): список шардов и параметры
// соединения читаются из side/queueds/multi в формате ключа queueds этого проекта.
func dial(ctx context.Context, root *onlineconf.Subtree, side string) (*queuedclient.QueuedClient, error) {
	conf := queueconfig.New(root.Subtree("/"+side), instanceName)

	client, err := queuedclient.Dial(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", side, err)
	}

	return client, nil
}

// reloadOnSignal просит супервизор перечитать конфиг по SIGHUP (tp.SignalReload).
func reloadOnSignal(ctx context.Context, m *mover.Mover, logger interface{ Info(string, ...any) }) {
	reload := make(chan os.Signal, 1)

	signal.Notify(reload, tp.SignalReload)
	defer signal.Stop(reload)

	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-reload:
			logger.Info("reloading config on signal", "signal", sig.String())
			m.Reload()
		}
	}
}
