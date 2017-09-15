/*
Package listener provides an SQS listener which calls a function on each message.
After the handler is complete, listener deletes the message.
*/
package listener

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/cache/lru"
	"github.com/twitchscience/aws_utils/logger"
)

type SQSHandler interface {
	Handle(*sqs.Message) error
}

// SQSFilter will perform a predicate on messages
type SQSFilter interface {
	Filter(*sqs.Message) bool
}

// DedupSQSFilter filters messages if they are a recent duplicate
type DedupSQSFilter struct {
	cache *lru.Cache
}

// Filter will return false to filter out messages if they are a recent duplicate
func (f *DedupSQSFilter) Filter(msg *sqs.Message) bool {
	msgBody := aws.StringValue(msg.Body)
	if !f.cache.Set(msgBody, "") {
		logger.WithField("message", msgBody).Info("Removing a duplicate")
		return false
	}
	return true
}

func NewDedupSQSFilter(maxEntries int, lifetime time.Duration) *DedupSQSFilter {
	return &DedupSQSFilter{cache: lru.New(maxEntries, lifetime)}
}

type SQSListener struct {
	Handler SQSHandler

	filter         SQSFilter
	sqsClient      sqsiface.SQSAPI
	pollInterval   time.Duration
	closeRequested chan bool
	closed         chan bool
}

func BuildSQSListener(handler SQSHandler, pollInterval time.Duration, client sqsiface.SQSAPI, filter SQSFilter) *SQSListener {
	return &SQSListener{
		filter:       filter,
		sqsClient:    client,
		pollInterval: pollInterval,
		Handler:      handler,
	}
}

func (l *SQSListener) Close() {
	close(l.closeRequested)
	<-l.closed
}

func (l *SQSListener) handle(msg *sqs.Message, qURL *string) {
	if l.filter != nil && !l.filter.Filter(msg) {
		return
	}
	err := l.Handler.Handle(msg)
	if err != nil {
		fmt.Printf("SQS Handler returned error: %s\n", err)

		_, err = l.sqsClient.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          qURL,
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: aws.Int64(10), // seconds
		})

		if err != nil {
			fmt.Printf("Error setting message visibility: %s\n", err)
		}
		return
	}

	_, err = l.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		fmt.Printf("Delete message failed with %s, message: %v", err, msg)
	}
}

func (l *SQSListener) waitForMessages(qURL *string) {
	o, err := l.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            qURL,
	})
	if err != nil || len(o.Messages) < 1 {
		time.Sleep(l.pollInterval)
		return
	}
	l.handle(o.Messages[0], qURL)
}

func (l *SQSListener) process(qURL *string) {
	for {
		select {
		case <-l.closeRequested:
			return
		default:
			l.waitForMessages(qURL)
		}
	}
}

func (l *SQSListener) Listen(qName string) {
	l.closeRequested = make(chan bool)
	l.closed = make(chan bool)

	defer close(l.closed)
	o, err := l.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		fmt.Printf("Error getting URL for SQS queue %s: %v", qName, err)
		return
	}

	l.process(o.QueueUrl)
}
