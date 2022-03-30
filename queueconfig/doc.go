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

		oom_soft_limit          OOMSoftLimit         https://pkg.go.dev/kythe.io/kythe/go/util/datasize#Parse
		oom_hard_limit          OOMHardLimit         https://pkg.go.dev/kythe.io/kythe/go/util/datasize#Parse
		oom_soft_downscale_rate OOMSoftDownscaleRate strconv.ParseFloat
		oom_check_period        OOMCheckPeriod       time.ParseDuration
		workers_max             WorkersMaxCount      strconv.ParseInt
		queue_ids               QueueIDs             comma-separated strconv.ParseUint
		log_level               LogLevel             zerolog.ParseLevel

Per-queue (название параметра, метод и формат, методы принимают queueID uint16):

		workers, workers_max_per_queue WorkersCount    strconv.ParseInt
		defer_time                     DeferTime       time.ParseDuration
		initial_delay                  InitialDelay    time.ParseDuration
		fetch_batch_size               FetchBatchSize  strconv.ParseInt
		update_batch_size              UpdateBatchSize strconv.ParseInt

Подробно конфигурационные значения описаны в README.md

Также, объект Config поддерживает интерфейс oconf.OnlineConf, и с его помощью можно обращаться и к значениям, заданным вне пути $ProjectRoot/tp/go (внутри $ProjectRoot).
*/
package queueconfig
