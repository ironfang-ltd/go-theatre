package theater

type Inbox struct {
	rb *RingBuffer[InboxMessage]
}

func NewInbox(size int64) *Inbox {
	return &Inbox{
		rb: NewRingBuffer[InboxMessage](size),
	}
}

func (i *Inbox) Send(msg InboxMessage) error {
	return i.rb.Write(msg)
}
