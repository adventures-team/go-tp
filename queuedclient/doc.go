/*
Package queuedclient - низкоуровневый клиент queued.

Высокоуровневые аналоги методов queuedclient.QueuedClient:

		GetActive  tp.Queue.GetActiveEvents
		GetByIDs   tp.Queue.GetEventsByIDs
		Delete     tp.Event.Done + tp.eventList.commit
		Defer      tp.Event.Defer (кастомный), tp.eventList.commit (дефолтный)
		Publish    tp.Queue.Publish, tp.Event.Publish
*/
package queuedclient
