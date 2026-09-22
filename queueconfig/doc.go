/*
Package queueconfig - доступ к конфигу с возможностью переопределения значений для очереди и инстанса.

	config := New(oconf, InstanceName)

Предположим, oconf указывает на некий $ProjectRoot. Существует три уровня, где можно задать значения конфига:

	global:       $ProjectRoot/tp/go
	per-instance: $ProjectRoot/tp/go/$InstanceName
	per-queue:    $ProjectRoot/tp/go/$InstanceName/$QueueID

Существует две группы значений - которые можно задать только per-instance, и per-queue.
Значения, задаваемые для очереди, переопределяют значения, заданные для инстанса, которые, в свою очередь, переопределяют глобальные значения.

Per-instance (название параметра, метод и формат):

	oom_soft_limit          OOMSoftLimit         размер: голое число - байты; kB/MB/GB/TB/PB - степени 1000; KiB/MiB/GiB/TiB/PiB - степени 1024
	oom_hard_limit          OOMHardLimit         размер (формат - как у oom_soft_limit)
	oom_soft_downscale_rate OOMSoftDownscaleRate strconv.ParseFloat
	oom_check_period        OOMCheckPeriod       time.ParseDuration
	workers_max             WorkersMaxCount      strconv.ParseInt
	queue_ids               QueueIDs             comma-separated strconv.ParseUint
	log_level               LogLevel             advslog.ParseLevel

Per-queue (название параметра, метод и формат, методы принимают queueID uint16):

	workers, workers_max_per_queue WorkersCount    strconv.ParseInt
	defer_time                     DeferTime       time.ParseDuration
	initial_delay                  InitialDelay    time.ParseDuration
	fetch_batch_size               FetchBatchSize  strconv.ParseInt
	update_batch_size              UpdateBatchSize strconv.ParseInt

Подробно конфигурационные значения описаны в README.md.

Также, объект [Config] поддерживает интерфейс [OnlineConf], и с его помощью можно обращаться и к произвольным значениям вне конфигурации очередей.

Для параметров, у которых есть дефолтные значения, ошибка не возвращается. Для логгирования проблем с такими параметрами (синтаксические
ошибки, и т.д.) соответствующим методом можно передать контекст. Если не требуется логгирование через контекст,
вместо контекста можно передать nil (используется [advslog.Ctx]).

Во избежание избыточного логгирования, про каждый ошибочный параметр в лог пишется только один раз. При обновлении
конфига/мягком рестарте следует вызвать метод [Config.ClearErrCache] для очистки списка путей, про которые была запись в лог об ошибке.

Методы, возвращающие обязательные параметры, ошибку возвращают, в случае получения ошибки следует прервать выполнение
(при первом запуске) или проигнорировать изменения в конфиге (при обновлении конфига в процессе работы).
*/
package queueconfig
